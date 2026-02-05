from langchain.tools import tool
from sqlalchemy import select

from app.database.session import SessionLocal
from app.database.schema import (
    company_ratios,
    annual_financials,
    quarterly_financials,
    shareholding_pattern,
    fundamental_status
)
from app.services.background import fetch_and_store_fundamentals
from app.services.fundamentals import is_data_fresh
from app.config.settings import settings


@tool
def ingest_fundamentals(ticker: str) -> str:
    """
    Scrape Screener.in and store fundamentals for a ticker.
    """
    fetch_and_store_fundamentals(ticker.upper())
    return f"Ingestion triggered for {ticker}"


@tool
def get_fundamentals(ticker: str) -> dict:
    """
    Fetch stored fundamentals from database.
    """
    db = SessionLocal()
    try:
        ratios = db.execute(
            select(company_ratios).where(company_ratios.c.ticker == ticker)
        ).mappings().all()

        annual = db.execute(
            select(annual_financials).where(annual_financials.c.ticker == ticker)
        ).mappings().all()

        quarterly = db.execute(
            select(quarterly_financials).where(quarterly_financials.c.ticker == ticker)
        ).mappings().all()

        shareholding = db.execute(
            select(shareholding_pattern).where(shareholding_pattern.c.ticker == ticker)
        ).mappings().all()

        profit_loss = []
        balance_sheet = []
        cash_flows = []

        for row in annual:
            metric = row.get("metric") or ""
            if metric.startswith("balance_sheet:"):
                balance_sheet.append({**row, "metric": metric.replace("balance_sheet:", "", 1)})
            elif metric.startswith("cash_flow:"):
                cash_flows.append({**row, "metric": metric.replace("cash_flow:", "", 1)})
            else:
                profit_loss.append(row)

        return {
            "ratios": ratios,
            "annual_financials": profit_loss,
            "balance_sheet": balance_sheet,
            "cash_flows": cash_flows,
            "quarterly": quarterly,
            "shareholding_pattern": shareholding
        }
    finally:
        db.close()


@tool
def check_data_status(ticker: str) -> dict:
    """
    Check ingestion status and freshness.
    """
    db = SessionLocal()
    try:
        row = db.execute(
            select(fundamental_status)
            .where(fundamental_status.c.ticker == ticker)
        ).mappings().first()

        if not row:
            return {"status": "NOT_STARTED"}

        stale = not is_data_fresh(db, ticker, settings.DATA_TTL_DAYS)

        return {
            "status": row["status"],
            "stale": stale,
            "progress": row["progress_pct"]
        }
    finally:
        db.close()
