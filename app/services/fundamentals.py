from datetime import date, timedelta
from sqlalchemy import select, func
from app.database.schema import company_ratios

def is_data_fresh(db, ticker: str, ttl_days: int) -> bool:
    stmt = (
        select(func.max(company_ratios.c.scraped_at))
        .where(company_ratios.c.ticker == ticker)
    )
    last_date = db.execute(stmt).scalar()

    if not last_date:
        return False

    return last_date >= (date.today() - timedelta(days=ttl_days))
