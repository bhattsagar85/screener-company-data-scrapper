import re
from sqlalchemy import select, case
from langchain_openai import ChatOpenAI

from app.database.session import SessionLocal
from app.database.schema import company_ratios
from app.services.background import fetch_and_store_fundamentals


# --------------------------------------------------
# Metric aliases & priority
# --------------------------------------------------

METRIC_ALIASES = {
    "OPM %": ["OPM %", "OPM", "OPM (Derived)"],
    "OPM": ["OPM (Derived)", "OPM %", "OPM"],
    "P/E": ["P/E", "PE"],
    "EPS": ["EPS", "EPS in Rs"],
}

# Priority order: first item = highest quality
METRIC_PRIORITY = {
    "OPM %": ["OPM (Derived)", "OPM %", "OPM"],
    "OPM": ["OPM (Derived)", "OPM %", "OPM"],
    "P/E": ["P/E"],
    "EPS": ["EPS", "EPS in Rs"],
}


def normalize_metric(metric: str) -> str:
    metric = metric.upper()
    metric = metric.replace("%", " %")
    metric = re.sub(r"\s+", " ", metric)
    return metric.strip()


# --------------------------------------------------
# Node: Parse query
# --------------------------------------------------

def parse_query(state: dict) -> dict:
    query = state["query"].upper()

    # ---- Extract ticker (last ALL CAPS word) ----
    tickers = re.findall(r"\b[A-Z]{2,}\b", query)
    ticker = tickers[-1] if tickers else None

    # ---- Metric synonyms ----
    METRIC_SYNONYMS = {
        "P/E": [
            "PRICE TO EARNING",
            "PRICE TO EARNINGS",
            "PE RATIO",
            "P E RATIO",
            "P/E",
            "PE",
        ],
        "OPM %": [
            "OPM",
            "OPM %",
            "OPERATING PROFIT MARGIN",
            "OPERATING MARGIN",
        ],
        "EPS": [
            "EPS",
            "EARNINGS PER SHARE",
        ],
    }

    metric = None
    for canonical, phrases in METRIC_SYNONYMS.items():
        for phrase in phrases:
            if phrase in query:
                metric = canonical
                break
        if metric:
            break

    if not ticker or not metric:
        raise ValueError(
            f"Could not parse ticker or metric from query: ticker={ticker}, metric={metric}"
        )

    return {
        **state,
        "ticker": ticker,
        "metric": metric,
    }



# --------------------------------------------------
# Node: Ingest fundamentals (idempotent)
# --------------------------------------------------

from datetime import date, timedelta
from sqlalchemy import select

from app.database.schema import fundamental_status
from app.database.session import SessionLocal
from app.services.background import fetch_and_store_fundamentals


# --------------------------------------------------
# Node: Ingest fundamentals (SMART + IDPOTENT)
# --------------------------------------------------

def ingest_data(state: dict) -> dict:
    ticker = state["ticker"]
    db = SessionLocal()

    try:
        row = db.execute(
            select(
                fundamental_status.c.last_updated,
                fundamental_status.c.status
            ).where(fundamental_status.c.ticker == ticker)
        ).first()

        # ---- Decide freshness window ----
        FRESH_DAYS = 7  # change to 7 if you want weekly refresh

        if row:
            last_updated, status = row
            is_fresh = (
                status == "COMPLETE" and
                last_updated >= date.today() - timedelta(days=FRESH_DAYS)
            )

            if is_fresh:
                # ✅ Skip ingestion
                return state

        # ❌ Missing or stale → ingest
        fetch_and_store_fundamentals(ticker)
        return state

    finally:
        db.close()



# --------------------------------------------------
# Node: Answer  ✅ FIXED
# --------------------------------------------------


def answer(state: dict) -> dict:
    ticker = state["ticker"]
    metric = state["metric"]

    # Normalize for DB search
    metric_norm = metric.replace(" ", "").replace("%", "").upper()

    db = SessionLocal()
    try:
        row = db.execute(
            select(
                company_ratios.c.metric,
                company_ratios.c.value
            )
            .where(company_ratios.c.ticker == ticker)
            .where(
                company_ratios.c.metric.ilike(f"%{metric_norm}%")
            )
            .order_by(company_ratios.c.scraped_at.desc())
            .limit(1)
        ).first()
    finally:
        db.close()

    if not row:
        return {
            **state,
            "answer": f"No data found for {metric} of {ticker}"
        }

    metric_name, value = row

    # Optional explanation
    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
    explanation = llm.invoke(
        f"Explain what {metric_name} = {value} means for {ticker} in simple terms."
    ).content

    return {
        **state,
        "answer": f"{ticker} has {metric_name} of {value}. {explanation}"
    }

