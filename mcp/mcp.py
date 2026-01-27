from langchain.tools import tool
import requests

BASE_URL = "http://127.0.0.1:8000"


@tool
def get_fundamentals(ticker: str, force: bool = False) -> dict:
    """
    Fetch fundamental data for an Indian stock ticker.
    Set force=true to re-ingest data from Screener.
    """
    resp = requests.get(
        f"{BASE_URL}/fundamentals/{ticker}",
        params={"force": force},
        timeout=30
    )
    resp.raise_for_status()
    return resp.json()
