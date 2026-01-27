from sqlalchemy import (
    Table,
    Column,
    String,
    Float,
    Date,
    Integer,
    MetaData,
    PrimaryKeyConstraint
)

from app.database.engine import engine

metadata = MetaData()

# -------------------------------------------------
# Company Ratios (Top ratios + Derived ratios)
# -------------------------------------------------

company_ratios = Table(
    "company_ratios",
    metadata,
    Column("ticker", String, nullable=False),
    Column("scraped_at", Date, nullable=False),
    Column("metric", String, nullable=False),

    # 🔑 NEW — exact Screener text
    Column("raw_value", String),

    # Parsed numeric value (if applicable)
    Column("value", Float),

    PrimaryKeyConstraint("ticker", "scraped_at", "metric")
)

# -------------------------------------------------
# Annual Financials (P&L + BS + CF)
# -------------------------------------------------

annual_financials = Table(
    "annual_financials",
    metadata,
    Column("ticker", String, nullable=False),
    Column("fiscal_year", String, nullable=False),
    Column("metric", String, nullable=False),
    Column("value", Float),
    PrimaryKeyConstraint("ticker", "fiscal_year", "metric")
)

# -------------------------------------------------
# Quarterly Financials
# -------------------------------------------------

quarterly_financials = Table(
    "quarterly_financials",
    metadata,
    Column("ticker", String, nullable=False),
    Column("quarter", String, nullable=False),
    Column("metric", String, nullable=False),
    Column("value", Float),
    PrimaryKeyConstraint("ticker", "quarter", "metric")
)

# -------------------------------------------------
# Shareholding Pattern
# -------------------------------------------------

shareholding_pattern = Table(
    "shareholding_pattern",
    metadata,
    Column("ticker", String, nullable=False),
    Column("period", String, nullable=False),
    Column("holder", String, nullable=False),
    Column("percentage", Float),
    PrimaryKeyConstraint("ticker", "period", "holder")
)

# -------------------------------------------------
# Raw HTML Snapshots
# -------------------------------------------------

raw_snapshots = Table(
    "raw_snapshots",
    metadata,
    Column("ticker", String, nullable=False),
    Column("scraped_at", Date, nullable=False),
    Column("section", String, nullable=False),
    Column("raw_html", String),
    PrimaryKeyConstraint("ticker", "scraped_at", "section")
)

# -------------------------------------------------
# Ingestion Status + Progress Tracking
# -------------------------------------------------

fundamental_status = Table(
    "fundamental_status",
    metadata,
    Column("ticker", String, primary_key=True),
    Column("status", String, nullable=False),
    Column("last_updated", Date),
    Column("error_message", String),

    Column("ratios_done", Integer, default=0),
    Column("quarterly_done", Integer, default=0),
    Column("annual_done", Integer, default=0),
    Column("shareholding_done", Integer, default=0),
    Column("derived_done", Integer, default=0),
    Column("progress_pct", Integer, default=0),
)

# -------------------------------------------------
# Create Tables
# -------------------------------------------------

def create_tables():
    metadata.create_all(engine)
