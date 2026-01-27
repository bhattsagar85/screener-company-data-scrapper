from pathlib import Path
from pydantic_settings import BaseSettings

BASE_DIR = Path(__file__).resolve().parents[2]

class Settings(BaseSettings):
    APP_NAME: str = "Screener Fundamentals Service"

    DATABASE_URL: str = f"sqlite:///{BASE_DIR}/data/screener.db"

    SCREENER_BASE_URL: str = "https://www.screener.in/company"
    USER_AGENT: str = "Mozilla/5.0 (compatible; ScreenerBot/1.0)"
    REQUEST_DELAY_SECONDS: int = 2

    DATA_TTL_DAYS: int = 30
    GLOBAL_RATE_LIMIT_SECONDS: int = 2

settings = Settings()
