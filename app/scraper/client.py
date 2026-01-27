import time
import requests
from requests.exceptions import HTTPError

from app.config.settings import settings


def fetch_company_page(ticker: str, consolidated: bool = True):
    """
    Fetch company page HTML.
    Consolidated is DEFAULT.
    """

    if consolidated:
        url = f"{settings.SCREENER_BASE_URL}/{ticker}/consolidated/"
    else:
        url = f"{settings.SCREENER_BASE_URL}/{ticker}/"

    headers = {
        "User-Agent": settings.USER_AGENT
    }

    time.sleep(settings.REQUEST_DELAY_SECONDS)

    response = requests.get(url, headers=headers, timeout=30)

    # Explicit failure for fallback logic
    response.raise_for_status()

    return response.text
