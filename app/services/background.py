from datetime import date
import re
import time
import logging
from collections import OrderedDict
import io

import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy.exc import IntegrityError
from sqlalchemy import select

from app.database.session import SessionLocal
from app.database.schema import (
    company_ratios,
    quarterly_financials,
    annual_financials,
    shareholding_pattern,
    raw_snapshots,
    fundamental_status
)
from app.scraper.client import fetch_company_page


# -------------------------------------------------
# Logging
# -------------------------------------------------

logger = logging.getLogger("fundamentals")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# -------------------------------------------------
# Retry Configuration
# -------------------------------------------------

MAX_RETRIES = 3
BASE_BACKOFF_SECONDS = 2

# -------------------------------------------------
# Helpers
# -------------------------------------------------

def clean_number(value):
    if value is None:
        return None
    value = str(value).replace("\xa0", " ")
    value = re.sub(r"[₹,%]", "", value)
    value = value.replace("Cr.", "").replace("Cr", "")
    value = value.replace(",", "").strip()
    return float(value) if re.match(r"^-?\d+(\.\d+)?$", value) else None


def safe_div(n, d):
    return round(n / d, 2) if n is not None and d not in (None, 0) else None


def is_fiscal_year(label: str) -> bool:
    return bool(re.match(r"^Mar\s\d{4}$", label))


def normalize_label(text: str) -> str:
    if not text:
        return ""
    return (
        text.lower()
        .replace("\xa0", " ")
        .replace("+", "")
        .replace("%", "")
        .replace("(", "")
        .replace(")", "")
        .strip()
    )


# -------------------------------------------------
# Metric Normalization
# -------------------------------------------------

METRIC_ALIASES = {
    "sales": "Sales +",
    "expenses": "Expenses +",
    "net profit": "Net Profit +",
    "operating profit": "Operating Profit",
    "opm": "OPM %",
    "eps in rs": "EPS in Rs",
    "borrowings": "Borrowings +",
    "equity capital": "Equity Capital",
    "reserves": "Reserves",
}

def normalize_metric(metric: str) -> str:
    key = normalize_label(metric)
    return METRIC_ALIASES.get(key, metric.strip())


# -------------------------------------------------
# Status Tracking
# -------------------------------------------------

def update_status(
    db,
    ticker,
    status,
    error_message=None,
    ratios_done=0,
    quarterly_done=0,
    annual_done=0,
    shareholding_done=0,
    derived_done=0
):
    progress_pct = int(
        (ratios_done + quarterly_done + annual_done +
         shareholding_done + derived_done) / 5 * 100
    )

    values = dict(
        status=status,
        last_updated=date.today(),
        error_message=error_message,
        ratios_done=ratios_done,
        quarterly_done=quarterly_done,
        annual_done=annual_done,
        shareholding_done=shareholding_done,
        derived_done=derived_done,
        progress_pct=progress_pct
    )

    exists = db.execute(
        select(fundamental_status.c.ticker)
        .where(fundamental_status.c.ticker == ticker)
    ).first()

    if exists:
        db.execute(
            fundamental_status.update()
            .where(fundamental_status.c.ticker == ticker)
            .values(**values)
        )
    else:
        db.execute(
            fundamental_status.insert().values(
                ticker=ticker,
                **values
            )
        )

    db.commit()


# -------------------------------------------------
# Derived Metrics
# -------------------------------------------------

def compute_and_store_derived_metrics(db, ticker, scraped_at):
    logger.info(f"🔧 START derived metrics for {ticker}")

    rows = db.execute(
        select(
            annual_financials.c.fiscal_year,
            annual_financials.c.metric,
            annual_financials.c.value
        ).where(annual_financials.c.ticker == ticker)
    ).fetchall()

    if not rows:
        logger.warning(f"⚠️ No annual data for {ticker}")
        return False

    df = pd.DataFrame(rows, columns=["year", "metric", "value"])

    years = sorted(
        [y for y in df.year.unique() if is_fiscal_year(y)],
        key=lambda x: int(x.split()[-1])
    )

    if len(years) < 2:
        return False

    def get(metric, year):
        metric = normalize_metric(metric)
        r = df[(df.metric == metric) & (df.year == year)]
        return r.value.iloc[0] if not r.empty else None

    latest = years[-1]
    prev_3y = years[-4] if len(years) >= 4 else None

    derived = OrderedDict()

    sales = get("Sales +", latest)
    eps = get("EPS in Rs", latest)
    opm = get("OPM %", latest)

    if sales is not None:
        derived["Sales"] = sales
    if eps is not None:
        derived["EPS"] = eps
    if opm is not None:
        derived["OPM"] = opm

    borrowings = get("Borrowings +", latest)
    equity = get("Equity Capital", latest)
    reserves = get("Reserves", latest)

    if borrowings and equity and reserves:
        derived["Debt to Equity"] = safe_div(borrowings, equity + reserves)

    if prev_3y:
        s0 = get("Sales +", prev_3y)
        s3 = get("Sales +", latest)
        if s0 and s3:
            derived["Sales growth 3Years"] = round(
                ((s3 / s0) ** (1 / 3) - 1) * 100, 2
            )

        p0 = get("Net Profit +", prev_3y)
        p3 = get("Net Profit +", latest)
        if p0 is not None and p3 is not None and p0>0 and p3>0:
            derived["Profit Var 3Yrs"] = round(
                ((p3 / p0) ** (1 / 3) - 1) * 100, 2
            )

    for metric, value in derived.items():
        db.execute(
            company_ratios.delete().where(
                company_ratios.c.ticker == ticker,
                company_ratios.c.metric == metric
            )
        )
        db.execute(
            company_ratios.insert().values(
                ticker=ticker,
                scraped_at=scraped_at,
                metric=metric,
                raw_value=str(value),
                value=value
            )
        )

    db.commit()
    logger.info("✅ Derived metrics stored")
    return True


# -------------------------------------------------
# Annual Data Validation
# -------------------------------------------------

def has_valid_annual_data(soup: BeautifulSoup) -> bool:
    pl = soup.find("section", id="profit-loss")
    if not pl:
        return False

    table = pl.find("table", class_="data-table")
    if not table:
        return False

    try:
        df = pd.read_html(io.StringIO(str(table)))[0]
    except ValueError:
        return False

    return any(
        isinstance(c, str) and re.match(r"Mar\s\d{4}", c)
        for c in df.columns
    )


# -------------------------------------------------
# Core Ingestion
# -------------------------------------------------

def _run_ingestion_once(db, ticker, scraped_at):
    progress = dict(ratios=0, quarterly=0, annual=0, shareholding=0, derived=0)

    html = fetch_company_page(ticker, consolidated=True)
    soup = BeautifulSoup(html, "lxml")

    if not has_valid_annual_data(soup):
        logger.warning(f"⚠️ {ticker}: Consolidated has no usable annual data, falling back")
        html = fetch_company_page(ticker, consolidated=False)
        soup = BeautifulSoup(html, "lxml")
        logger.info(f"📊 {ticker} data scope: STANDALONE")
    else:
        logger.info(f"📊 {ticker} data scope: CONSOLIDATED")

    # ---------- RAW SNAPSHOT ----------
    db.execute(
        raw_snapshots.delete().where(
            raw_snapshots.c.ticker == ticker,
            raw_snapshots.c.scraped_at == scraped_at,
            raw_snapshots.c.section == "full_page"
        )
    )
    db.execute(
        raw_snapshots.insert().values(
            ticker=ticker,
            scraped_at=scraped_at,
            section="full_page",
            raw_html=html
        )
    )
    db.commit()

    # ---------- RATIOS ----------
    for li in soup.select("div.company-ratios ul#top-ratios li"):
        name = li.select_one("span.name")
        value = li.select_one("span.nowrap.value")
        if not name or not value:
            continue

        raw = value.get_text(" ", strip=True)
        metric = name.text.strip()

        db.execute(
            company_ratios.delete().where(
                company_ratios.c.ticker == ticker,
                company_ratios.c.metric == metric
            )
        )
        db.execute(
            company_ratios.insert().values(
                ticker=ticker,
                scraped_at=scraped_at,
                metric=metric,
                raw_value=raw,
                value=clean_number(raw)
            )
        )
        progress["ratios"] = 1

    db.commit()

    # ---------- QUARTERLY ----------
    sec = soup.find("section", id="quarters")
    if sec:
        table = sec.find("table", class_="data-table")
        if table:
            df = pd.read_html(io.StringIO(str(table)))[0]
            df = df.rename(columns={df.columns[0]: "metric"})
            melted = df.melt(id_vars="metric", var_name="quarter", value_name="value")
            melted["value"] = melted["value"].apply(clean_number)

            rows_seen = False
            for _, r in melted.iterrows():
                rows_seen = True
                try:
                    db.execute(
                        quarterly_financials.insert().values(
                            ticker=ticker,
                            quarter=r.quarter,
                            metric=normalize_metric(r.metric),
                            value=r.value
                        )
                    )
                except IntegrityError:
                    db.rollback()

            if rows_seen:
                db.commit()
                progress["quarterly"] = 1

    # ---------- ANNUAL ----------
    pl = soup.find("section", id="profit-loss")
    if pl:
        table = pl.find("table", class_="data-table")
        if table:
            df = pd.read_html(io.StringIO(str(table)))[0]
            df = df.rename(columns={df.columns[0]: "metric"})
            melted = df.melt(id_vars="metric", var_name="fiscal_year", value_name="value")
            melted["value"] = melted["value"].apply(clean_number)

            rows_seen = False
            for _, r in melted.iterrows():
                rows_seen = True
                try:
                    db.execute(
                        annual_financials.insert().values(
                            ticker=ticker,
                            fiscal_year=r.fiscal_year.strip(),
                            metric=normalize_metric(r.metric),
                            value=r.value
                        )
                    )
                except IntegrityError:
                    db.rollback()

            if rows_seen:
                db.commit()
                progress["annual"] = 1

    # ---------- SHAREHOLDING ----------
    sec = soup.find("section", id="shareholding")
    if sec:
        table = sec.find("table", class_="data-table")
        if table:
            df = pd.read_html(io.StringIO(str(table)))[0]
            df = df.rename(columns={df.columns[0]: "holder"})
            melted = df.melt(id_vars="holder", var_name="period", value_name="percentage")
            melted["percentage"] = melted["percentage"].apply(clean_number)

            rows_seen = False
            for _, r in melted.iterrows():
                rows_seen = True
                try:
                    db.execute(
                        shareholding_pattern.insert().values(
                            ticker=ticker,
                            period=r.period,
                            holder=r.holder,
                            percentage=r.percentage
                        )
                    )
                except IntegrityError:
                    db.rollback()

            if rows_seen:
                db.commit()
                progress["shareholding"] = 1

    if compute_and_store_derived_metrics(db, ticker, scraped_at):
        progress["derived"] = 1

    return progress


# -------------------------------------------------
# Background Entrypoint
# -------------------------------------------------

def fetch_and_store_fundamentals(ticker: str):
    db = SessionLocal()
    scraped_at = date.today()

    try:
        update_status(db, ticker, "IN_PROGRESS")

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                progress = _run_ingestion_once(db, ticker, scraped_at)

                update_status(
                    db,
                    ticker,
                    "COMPLETE",
                    ratios_done=progress["ratios"],
                    quarterly_done=progress["quarterly"],
                    annual_done=progress["annual"],
                    shareholding_done=progress["shareholding"],
                    derived_done=progress["derived"]
                )

                logger.info(f"✅ Ingestion COMPLETE for {ticker}")
                return

            except Exception as e:
                logger.warning(f"⚠️ Retry {attempt} failed: {e}")
                if attempt == MAX_RETRIES:
                    raise
                time.sleep(BASE_BACKOFF_SECONDS * (2 ** (attempt - 1)))

    except Exception as e:
        update_status(db, ticker, "FAILED", str(e))
        logger.error(f"❌ FAILED {ticker}: {e}")

    finally:
        db.close()
