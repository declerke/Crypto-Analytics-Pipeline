# 🪙 Crypto Market Analytics Pipeline: Production-Style ELT

**Crypto Market Analytics Pipeline** is a comprehensive data engineering solution designed to ingest, model, and visualize live cryptocurrency market data from the **Binance Public REST API**. This project implements a modular **ELT (Extract-Load-Transform)** architecture, moving from raw ingestion to analytics-ready data marts using **dbt (Data Build Tool)** and **PostgreSQL**.

---

## 🧬 System Architecture
The pipeline follows a modern "Source-to-Dashboard" logic:

1.  **Ingestion Layer:** Python-based service polls the Binance API every 60s to fetch candlesticks (klines), individual trades, and 24hr ticker stats.
2.  **Raw Storage (PostgreSQL):** Data is persisted in its native form into the `raw` schema to preserve a full audit trail.
3.  **Transformation Layer (dbt):**
    * **Staging:** Renames columns, converts Unix ms epochs to TIMESTAMPTZ, and casts string numerics to high-precision decimals.
    * **Intermediate:** Joins datasets and calculates enriched metrics like hourly price volatility.
    * **Marts:** Materializes final analytics-ready tables for performant reporting.
4.  **Visualization:** **Grafana** executes real-time queries against the dbt marts to populate auto-refreshing dashboards.

---

## 🧠 Key Design Decisions & Logic

* **Idempotent Ingestion (Upsert Strategy):**
    * **Klines:** Uses `ON CONFLICT (symbol, open_time) DO UPDATE` to refresh the "live" candle until it closes.
    * **Trades:** Uses `ON CONFLICT (symbol, trade_id) DO NOTHING` as trade records are immutable.
    * **Tickers:** Uses `ON CONFLICT (symbol) DO UPDATE` to maintain a single rolling snapshot per symbol.
* **Precision & Accuracy:** All price and volume columns use `NUMERIC(24, 8)` and `NUMERIC(36, 8)` to match Binance's maximum precision without floating-point rounding errors.
* **Materialization Strategy:** Staging and Intermediate layers are built as **Views** to ensure logic is always fresh; Mart models are **Tables** to optimize Grafana read performance.
* **Volatility Proxy:** Uses `(high - low) / low * 100` as a proxy for volatility, providing immediate interpretability without requiring long-term historical windows.

---

## 🛠️ Technical Stack
| Category | Tool | Version |
| :--- | :--- | :--- |
| **Ingestion** | Python + requests + SQLAlchemy | 3.11+ |
| **Database** | PostgreSQL | 16 |
| **Transformation** | dbt Core (postgres adapter) | 1.11+ |
| **Visualization** | Grafana OSS | Latest |
| **Orchestration** | Docker + Docker Compose | 3.8 |

---

## 📂 Project Structure
```text
crypto-analytics-pipeline/
├── docker-compose.yml        # Infrastructure orchestration
├── requirements.txt          # Python dependencies
├── .env.example              # Environment variables template
├── scripts/
│   └── ingest.py             # Binance → PostgreSQL ingestion service
├── docker/
│   ├── postgres-init/
│   │   └── init.sql          # Raw schema & table DDL
│   └── grafana/
│       └── provisioning/     # Automated dashboard & datasource setup
└── dbt_project/
    ├── dbt_project.yml       # dbt configuration
    ├── profiles.yml          # Connection settings
    └── models/
        ├── sources.yml       # Raw table definitions & source tests
        ├── staging/          # Layer 1: Cleaning & Standardizing
        ├── intermediate/     # Layer 2: Joins & Aggregations
        └── marts/            # Layer 3: Fact & Dimension tables
```

---

## 📊 Analytics Insights
The pipeline is designed to answer five core business questions:
1.  **Daily Average Price:** `avg_close_price` in `mart_daily_market_summary`.
2.  **Highest Trading Volume:** `volume_rank_on_day` identifies the leading coin.
3.  **Hourly Volatility:** Calculated in `int_price_metrics`.
4.  **Largest Price Movements:** `daily_price_range` tracking.
5.  **7-Day Trends:** `rolling_7day_avg_price` via window functions.

### **Dashboard & Pipeline Visualization**
![Lineage Graph](./grafana%20reports/Lineage%20Graph.png)
![Daily Average Close](./grafana%20reports/Daily%20Average%20Close.png)
![Hourly Volatility](./grafana%20reports/Hourly%20Price%20Volatility.png)

---

## ⚙️ Quick Start Guide

### 1. Clone and Configure
```bash
git clone <(https://github.com/declerke/Crypto-Analytics-Pipeline/tree/main)>
cd crypto-analytics-pipeline
cp .env.example .env
# Edit .env if you need to change default credentials
```

### 2. Start Infrastructure
```bash
docker compose up -d
# Verify services are healthy
docker compose ps
```
*PostgreSQL is ready at localhost:5433. Grafana is at http://localhost:3000 (admin / admin).*

### 3. Install Python Dependencies
```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 4. Start Data Ingestion
```bash
python scripts/ingest.py
```
*Leave this running. It will poll Binance every 60 seconds.*

### 5. Run and Test dbt Models
```bash
cd dbt_project
# Verify connection
dbt debug
# Build all models and run 56 data tests
dbt build
```

### 6. View Dashboards
Open `http://localhost:3000` → Dashboards → **Crypto Market Analytics Pipeline**.
The dashboard auto-refreshes every 30 seconds.

---

## 🎓 Skills Demonstrated
* **Production ELT Design:** Building structured layers for scalable analytics.
* **Data Quality Engineering:** Implementing schema-level tests and unique constraints.
* **Infrastructure as Code:** Automating the entire environment deployment.
```python
# Note: Ingestion scripts use specialized upsert logic to handle real-time data streaming.
```
