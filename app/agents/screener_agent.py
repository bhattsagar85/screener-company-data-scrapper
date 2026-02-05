from langchain_openai import ChatOpenAI
from langchain_core.tools import tool
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableLambda

from app.services.background import fetch_and_store_fundamentals
from app.database.session import SessionLocal
from app.database.schema import fundamental_status, company_ratios, annual_financials
from app.database.schema import shareholding_pattern
from sqlalchemy import select
from dotenv import load_dotenv
load_dotenv()


# -------------------------------------------------
# TOOLS (plain python functions!)
# -------------------------------------------------

@tool
def ingest_fundamentals(ticker: str) -> str:
    """
    Trigger background ingestion of fundamentals for a ticker.
    """
    fetch_and_store_fundamentals(ticker.upper())
    return f"Ingestion triggered for {ticker.upper()}"


@tool
def check_data_status(ticker: str) -> str:
    """
    Check ingestion status for a ticker.
    """
    db = SessionLocal()
    try:
        row = db.execute(
            select(fundamental_status)
            .where(fundamental_status.c.ticker == ticker.upper())
        ).mappings().first()

        if not row:
            return "No data found"

        return f"Status={row['status']}, Progress={row['progress_pct']}%"
    finally:
        db.close()


@tool
def get_fundamentals(ticker: str) -> dict:
    """
    Fetch stored fundamentals from DB.
    """
    db = SessionLocal()
    try:
        ratios = db.execute(
            select(company_ratios)
            .where(company_ratios.c.ticker == ticker.upper())
        ).mappings().all()

        annual = db.execute(
            select(annual_financials)
            .where(annual_financials.c.ticker == ticker.upper())
        ).mappings().all()

        shareholding = db.execute(
            select(shareholding_pattern)
            .where(shareholding_pattern.c.ticker == ticker.upper())
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
            "shareholding_pattern": shareholding
        }
    finally:
        db.close()


# -------------------------------------------------
# AGENT
# -------------------------------------------------

def get_screener_agent():
    llm = ChatOpenAI(
        model="gpt-4o-mini",
        temperature=0
    )

    system_prompt = """
You are a SEBI-style fundamental research analyst.
You analyze Indian stocks using stored fundamentals.
If data is missing, trigger ingestion first.
Explain answers in simple, clear terms.
"""

    prompt = ChatPromptTemplate.from_messages([
        ("system", system_prompt),
        ("human", "{input}")
    ])

    tools = [
        ingest_fundamentals,
        check_data_status,
        get_fundamentals
    ]

    chain = (
        prompt
        | llm.bind_tools(tools)
    )

    return chain
