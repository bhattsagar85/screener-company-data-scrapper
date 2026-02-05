# ---------- Base Image ----------
    FROM python:3.11-slim

    # ---------- Environment ----------
    ENV PYTHONDONTWRITEBYTECODE=1
    ENV PYTHONUNBUFFERED=1
    
    # ---------- System deps ----------
    RUN apt-get update && apt-get install -y \
        build-essential \
        curl \
        wget \
        ca-certificates \
        && rm -rf /var/lib/apt/lists/*
    
    # ---------- Working directory ----------
    WORKDIR /app
    
    # ---------- Install Python deps ----------
    COPY requirements.txt .
    RUN pip install --no-cache-dir -r requirements.txt
    
    # ---------- Copy application code ----------
    COPY . .
    
    # ---------- Expose service port ----------
    EXPOSE 8000
    
    # ---------- Healthcheck ----------
    HEALTHCHECK --interval=30s --timeout=5s --start-period=30s \
      CMD curl -f http://localhost:8000/health || exit 1
    
    # ---------- Run ----------
    CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
    