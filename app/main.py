from fastapi import FastAPI
from contextlib import asynccontextmanager

from app.database.schema import create_tables
from app.api.routes.fundamentals import router as fundamentals_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ---- Startup ----
    create_tables()
    print("✅ Database tables initialized")

    yield  # App runs here

    # ---- Shutdown ----
    print("🛑 Application shutdown")


app = FastAPI(
    title="Fundamental Data Service",
    lifespan=lifespan
)

# ✅ REGISTER ROUTES ON THE SAME APP
app.include_router(fundamentals_router)
