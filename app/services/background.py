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
# Config
# -------------------------------------------------

MAX_RETRIES = 3
BASE_BACKOFF_SECONDS = 2

# ❌ Ratios we NEVER trust from Screener top-ratios
IGNORED_RATIOS = {
    "Qtr Profit Var",
    "Qtr Sales Var",
}

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
    return METRIC_ALIASES.get(normalize_label(metric), metric.strip())


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
            fundamental_status.insert().values(ticker=ticker, **values)
        )

    db.commit()


# -------------------------------------------------
# Derived Metrics (CORRECT)
# -------------------------------------------------

def compute_qtr_profit_var(df_qtr):
    df = df_qtr[df_qtr.metric == "Net Profit +"].dropna(subset=["value"])

    if len(df) < 2:
        return None

    # Convert "Dec 2024" → datetime
    df["qtr_dt"] = pd.to_datetime(df["quarter"], format="%b %Y")

    df = df.sort_values("qtr_dt")

    latest = df.iloc[-1].value
    prev = df.iloc[-2].value

    if prev and latest:
        return round(((latest - prev) / prev) * 100, 2)

    return None



def compute_and_store_derived_metrics(db, ticker, scraped_at):
    logger.info(f"🔧 START derived metrics for {ticker}")

    # ---------- Annual ----------
    annual_rows = db.execute(
        select(
            annual_financials.c.fiscal_year,
            annual_financials.c.metric,
            annual_financials.c.value
        ).where(annual_financials.c.ticker == ticker)
    ).fetchall()

    # ---------- Quarterly ----------
    qtr_rows = db.execute(
        select(
            quarterly_financials.c.quarter,
            quarterly_financials.c.metric,
            quarterly_financials.c.value
        ).where(quarterly_financials.c.ticker == ticker)
    ).fetchall()

    if not annual_rows or not qtr_rows:
        return False

    df_annual = pd.DataFrame(annual_rows, columns=["year", "metric", "value"])
    df_qtr = pd.DataFrame(qtr_rows, columns=["quarter", "metric", "value"])

    years = sorted(
        [y for y in df_annual.year.unique() if is_fiscal_year(y)],
        key=lambda x: int(x.split()[-1])
    )

    if not years:
        return False

    latest = years[-1]

    def get(metric):
        r = df_annual[(df_annual.metric == metric) & (df_annual.year == latest)]
        return r.value.iloc[0] if not r.empty else None

    derived = OrderedDict()

    # ✅ Correct OPM (annual)
    opm = get("OPM %")
    if opm is not None:
        derived["OPM (Derived)"] = opm

    # ✅ Correct EPS
    eps = get("EPS in Rs")
    if eps is not None:
        derived["EPS"] = eps

    # ✅ Correct QoQ Profit Var (THIS FIXES 14.33 BUG)
    qtr_profit_var = compute_qtr_profit_var(df_qtr)
    if qtr_profit_var is not None:
        derived["Qtr Profit Var"] = qtr_profit_var

    # ---------- Store ----------
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
# Core Ingestion
# -------------------------------------------------

def _run_ingestion_once(db, ticker, scraped_at):
    progress = dict(ratios=0, quarterly=0, annual=0, shareholding=0, derived=0)

    html = fetch_company_page(ticker, consolidated=True)
    soup = BeautifulSoup(html, "lxml")

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

    # ---------- RATIOS (SAFE ONLY) ----------
    for li in soup.select("div.company-ratios ul#top-ratios li"):
        name = li.select_one("span.name")
        value = li.select_one("span.nowrap.value")
        if not name or not value:
            continue

        metric = name.text.strip()
        if metric in IGNORED_RATIOS:
            continue

        raw = value.get_text(" ", strip=True)

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

            for _, r in melted.iterrows():
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

            db.commit()
            progress["quarterly"] = 1

    # ---------- ANNUAL (P&L + BS + CF) ----------
    def ingest_annual_section(section_id: str, metric_prefix: str | None):
        sec = soup.find("section", id=section_id)
        if not sec:
            return False
        table = sec.find("table", class_="data-table")
        if not table:
            return False

        df = pd.read_html(io.StringIO(str(table)))[0]
        df = df.rename(columns={df.columns[0]: "metric"})
        melted = df.melt(id_vars="metric", var_name="fiscal_year", value_name="value")
        melted["value"] = melted["value"].apply(clean_number)

        for _, r in melted.iterrows():
            metric = normalize_metric(r.metric)
            if metric_prefix:
                metric = f"{metric_prefix}{metric}"
            try:
                db.execute(
                    annual_financials.insert().values(
                        ticker=ticker,
                        fiscal_year=str(r.fiscal_year).strip(),
                        metric=metric,
                        value=r.value
                    )
                )
            except IntegrityError:
                db.rollback()

        db.commit()
        return True

    annual_ok = False
    if ingest_annual_section("profit-loss", None):
        annual_ok = True
    if ingest_annual_section("balance-sheet", "balance_sheet:"):
        annual_ok = True
    if ingest_annual_section("cash-flow", "cash_flow:"):
        annual_ok = True

    if annual_ok:
        progress["annual"] = 1

    # ---------- SHAREHOLDING ----------
    sh = soup.find("section", id="shareholding") or soup.find("section", id="shareholding-pattern")
    if sh:
        table = sh.find("table", class_="data-table")
        if table:
            df = pd.read_html(io.StringIO(str(table)))[0]
            df = df.rename(columns={df.columns[0]: "holder"})
            melted = df.melt(id_vars="holder", var_name="period", value_name="percentage")
            melted["percentage"] = melted["percentage"].apply(clean_number)

            for _, r in melted.iterrows():
                try:
                    db.execute(
                        shareholding_pattern.insert().values(
                            ticker=ticker,
                            period=str(r.period).strip(),
                            holder=str(r.holder).strip(),
                            percentage=r.percentage
                        )
                    )
                except IntegrityError:
                    db.rollback()

            db.commit()
            progress["shareholding"] = 1

    # ---------- DERIVED ----------
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
