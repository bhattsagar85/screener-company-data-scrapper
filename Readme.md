**Fundamental Data Service**

This repo provides a FastAPI service that scrapes and stores Indian stock fundamentals from Screener.in, then exposes them via REST endpoints. It ingests ratios, quarterly results, annual financials (P&L, balance sheet, cash flow), and shareholding patterns. It also includes a lightweight agent endpoint for natural‑language Q&A over stored fundamentals.

**What It Does**
- Scrapes fundamentals for a given ticker from Screener.in.
- Stores data in a local SQLite DB (`data/screener.db`).
- Exposes REST endpoints for raw fundamentals and ingestion status.
- Supports optional agent Q&A (requires OpenAI API key).

**Tech Stack**
- FastAPI + Uvicorn
- SQLAlchemy + SQLite
- BeautifulSoup + Pandas
- LangChain + LangGraph (agent)

**Quick Start (Docker)**
Build the image:

```bash
docker build -t fundamentals-ingestion .
```

Run the container:

```bash
docker run -p 8002:8000 fundamentals-ingestion
```

**Local Run (Python)**
Create a virtualenv and install deps:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Run the API:

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

**Environment Variables**
Optional, only needed for the agent endpoint:

```bash
OPENAI_API_KEY=...
```

You can also place these in `.env`.

**API Endpoints**
- `GET /fundamentals/{ticker}`  
  Returns ratios, quarterly results, annual financials (P&L), balance sheet, cash flows, and shareholding pattern.  
  Query param: `force=true` triggers re‑ingestion.

- `GET /fundamentals/{ticker}/status`  
  Returns ingestion status and progress for a ticker.

- `POST /fundamentals/status/bulk`  
  Request body: array of tickers, returns status for each.

- `POST /fundamentals/ingest/bulk`  
  Request body: array of tickers, triggers ingestion when needed.

- `POST /agent/ask`  
  Request body: `{ "query": "..." }`

**Example Requests**
Fetch fundamentals:

```bash
curl -X 'GET' \
  'http://localhost:8002/fundamentals/SUZLON?force=false' \
  -H 'accept: application/json'
```

Trigger ingestion:

```bash
curl -X 'GET' \
  'http://localhost:8002/fundamentals/SUZLON?force=true' \
  -H 'accept: application/json'
```

Ask the agent:

```bash
curl -X 'POST' \
  'http://localhost:8002/agent/ask' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{"query":"What is the OPM of SUZLON?"}'
```

**Notes**
- Ingestion runs in the background; check status if a ticker is still processing.
- Data freshness is controlled by `DATA_TTL_DAYS` in `app/config/settings.py`.
