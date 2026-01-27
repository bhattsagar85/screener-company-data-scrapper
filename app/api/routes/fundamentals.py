from fastapi import APIRouter, BackgroundTasks, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import select

from app.database.session import SessionLocal
from app.database.schema import (
    company_ratios,
    quarterly_financials,
    annual_financials,
    fundamental_status
)
from app.services.background import fetch_and_store_fundamentals
from app.services.fundamentals import is_data_fresh
from app.config.settings import settings

router = APIRouter(prefix="/fundamentals", tags=["Fundamentals"])


# -------------------------------------------------
# DB Dependency
# -------------------------------------------------

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# -------------------------------------------------
# GET FULL FUNDAMENTALS (FULL-ONLY)
# -------------------------------------------------

@router.get("/{ticker}")
def get_fundamentals(
    ticker: str,
    background_tasks: BackgroundTasks,
    force: bool = Query(False, description="Force re-ingestion"),
    db: Session = Depends(get_db)
):
    ticker = ticker.upper()

    # -------------------------------------------------
    # FORCE OVERRIDE (developer-friendly)
    # -------------------------------------------------
    if force:
        background_tasks.add_task(fetch_and_store_fundamentals, ticker)
        return {
            "ticker": ticker,
            "status": "forced",
            "message": "Force re-ingestion triggered in background"
        }

    status_row = db.execute(
        select(fundamental_status)
        .where(fundamental_status.c.ticker == ticker)
    ).mappings().first()

    if not status_row or status_row["status"] in ("PENDING", "FAILED"):
        background_tasks.add_task(fetch_and_store_fundamentals, ticker)
        return {
            "ticker": ticker,
            "status": "processing",
            "message": "Fundamentals ingestion started."
        }

    if status_row["status"] == "IN_PROGRESS":
        return {
            "ticker": ticker,
            "status": "processing",
            "progress_pct": status_row["progress_pct"],
            "sections": {
                "ratios": bool(status_row["ratios_done"]),
                "quarterly": bool(status_row["quarterly_done"]),
                "annual": bool(status_row["annual_done"]),
                "shareholding": bool(status_row["shareholding_done"]),
                "derived": bool(status_row["derived_done"])
            }
        }

    if not is_data_fresh(db, ticker, settings.DATA_TTL_DAYS):
        background_tasks.add_task(fetch_and_store_fundamentals, ticker)
        return {
            "ticker": ticker,
            "status": "stale",
            "message": "Data is stale. Refresh in progress.",
            "progress_pct": status_row["progress_pct"]
        }

    ratios = db.execute(
        select(company_ratios)
        .where(company_ratios.c.ticker == ticker)
    ).mappings().all()

    quarterly = db.execute(
        select(quarterly_financials)
        .where(quarterly_financials.c.ticker == ticker)
    ).mappings().all()

    annual = db.execute(
        select(annual_financials)
        .where(annual_financials.c.ticker == ticker)
    ).mappings().all()

    return {
        "ticker": ticker,
        "status": "ready",
        "progress_pct": 100,
        "ratios": ratios,
        "quarterly_results": quarterly,
        "annual_financials": annual
    }


# -------------------------------------------------
# GET INGESTION STATUS (SINGLE – WITH PROGRESS)
# -------------------------------------------------

@router.get("/{ticker}/status")
def get_fundamental_status(
    ticker: str,
    db: Session = Depends(get_db)
):
    ticker = ticker.upper()

    row = db.execute(
        select(fundamental_status)
        .where(fundamental_status.c.ticker == ticker)
    ).mappings().first()

    if not row:
        return {
            "ticker": ticker,
            "status": "NOT_STARTED",
            "progress_pct": 0,
            "sections": {
                "ratios": False,
                "quarterly": False,
                "annual": False,
                "shareholding": False,
                "derived": False
            }
        }

    return {
        "ticker": ticker,
        "status": row["status"],
        "last_updated": row["last_updated"],
        "progress_pct": row["progress_pct"],
        "sections": {
            "ratios": bool(row["ratios_done"]),
            "quarterly": bool(row["quarterly_done"]),
            "annual": bool(row["annual_done"]),
            "shareholding": bool(row["shareholding_done"]),
            "derived": bool(row["derived_done"])
        },
        "error_message": row["error_message"]
    }


# -------------------------------------------------
# BULK INGESTION STATUS (WITH PROGRESS)
# -------------------------------------------------

@router.post("/status/bulk")
def get_bulk_fundamental_status(
    tickers: list[str],
    db: Session = Depends(get_db)
):
    tickers = [t.upper() for t in tickers]

    rows = db.execute(
        select(fundamental_status)
        .where(fundamental_status.c.ticker.in_(tickers))
    ).mappings().all()

    status_map = {row["ticker"]: row for row in rows}
    results = []

    for ticker in tickers:
        row = status_map.get(ticker)

        if not row:
            results.append({
                "ticker": ticker,
                "status": "NOT_STARTED",
                "progress_pct": 0,
                "sections": {
                    "ratios": False,
                    "quarterly": False,
                    "annual": False,
                    "shareholding": False,
                    "derived": False
                }
            })
        else:
            results.append({
                "ticker": ticker,
                "status": row["status"],
                "progress_pct": row["progress_pct"],
                "sections": {
                    "ratios": bool(row["ratios_done"]),
                    "quarterly": bool(row["quarterly_done"]),
                    "annual": bool(row["annual_done"]),
                    "shareholding": bool(row["shareholding_done"]),
                    "derived": bool(row["derived_done"])
                },
                "error_message": row["error_message"]
            })

    return {
        "count": len(results),
        "results": results
    }


# -------------------------------------------------
# BULK TRIGGER INGESTION
# -------------------------------------------------

@router.post("/ingest/bulk")
def bulk_trigger_ingestion(
    tickers: list[str],
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    tickers = list(set(t.upper() for t in tickers))
    triggered = []

    for ticker in tickers:
        row = db.execute(
            select(fundamental_status)
            .where(fundamental_status.c.ticker == ticker)
        ).mappings().first()

        if not row or row["status"] in ("NOT_STARTED", "FAILED"):
            background_tasks.add_task(fetch_and_store_fundamentals, ticker)
            triggered.append(ticker)

    return {
        "requested": len(tickers),
        "triggered": len(triggered),
        "tickers": triggered
    }
