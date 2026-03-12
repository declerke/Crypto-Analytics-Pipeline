import os
import time
import logging
import requests
import schedule
from datetime import datetime, timezone
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

POSTGRES_HOST     = os.getenv("POSTGRES_HOST", "127.0.0.1").strip()
POSTGRES_PORT     = os.getenv("POSTGRES_PORT", "5433").strip() # Change to 5433 if port 5432 is in use
POSTGRES_DB       = os.getenv("POSTGRES_DB", "crypto_db").strip()
POSTGRES_USER     = os.getenv("POSTGRES_USER", "crypto_user").strip()
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "crypto_pass").strip()

BINANCE_BASE_URL  = os.getenv("BINANCE_BASE_URL", "https://api.binance.com")
SYMBOLS           = os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,BNBUSDT").split(",")
KLINES_INTERVAL   = os.getenv("KLINES_INTERVAL", "1m")
KLINES_LIMIT      = int(os.getenv("KLINES_LIMIT", "500"))
TRADES_LIMIT      = int(os.getenv("TRADES_LIMIT", "500"))
POLL_INTERVAL     = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))

DATABASE_URL = (
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

def get_engine():
    return create_engine(DATABASE_URL, pool_pre_ping=True)

def fetch_klines(symbol: str) -> list[list]:
    url = f"{BINANCE_BASE_URL}/api/v3/klines"
    params = {"symbol": symbol, "interval": KLINES_INTERVAL, "limit": KLINES_LIMIT}
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    return response.json()

def fetch_trades(symbol: str) -> list[dict]:
    url = f"{BINANCE_BASE_URL}/api/v3/trades"
    params = {"symbol": symbol, "limit": TRADES_LIMIT}
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    return response.json()

def fetch_ticker(symbol: str) -> dict:
    url = f"{BINANCE_BASE_URL}/api/v3/ticker/24hr"
    params = {"symbol": symbol}
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    return response.json()

def load_klines(engine, symbol: str, rows: list[list]) -> int:
    if not rows:
        return 0

    upsert_sql = text("""
        INSERT INTO raw.raw_binance_klines (
            symbol, open_time, open, high, low, close, volume,
            close_time, quote_asset_volume, number_of_trades,
            taker_buy_base_asset_volume, taker_buy_quote_asset_volume,
            ingested_at
        ) VALUES (
            :symbol, :open_time, :open, :high, :low, :close, :volume,
            :close_time, :quote_asset_volume, :number_of_trades,
            :taker_buy_base_asset_volume, :taker_buy_quote_asset_volume,
            :ingested_at
        )
        ON CONFLICT (symbol, open_time)
        DO UPDATE SET
            open                         = EXCLUDED.open,
            high                         = EXCLUDED.high,
            low                          = EXCLUDED.low,
            close                        = EXCLUDED.close,
            volume                       = EXCLUDED.volume,
            close_time                   = EXCLUDED.close_time,
            quote_asset_volume           = EXCLUDED.quote_asset_volume,
            number_of_trades             = EXCLUDED.number_of_trades,
            taker_buy_base_asset_volume  = EXCLUDED.taker_buy_base_asset_volume,
            taker_buy_quote_asset_volume = EXCLUDED.taker_buy_quote_asset_volume,
            ingested_at                  = EXCLUDED.ingested_at
    """)

    ingested_at = datetime.now(timezone.utc)
    records = [
        {
            "symbol":                       symbol,
            "open_time":                    int(row[0]),
            "open":                         row[1],
            "high":                         row[2],
            "low":                          row[3],
            "close":                        row[4],
            "volume":                       row[5],
            "close_time":                   int(row[6]),
            "quote_asset_volume":           row[7],
            "number_of_trades":             int(row[8]),
            "taker_buy_base_asset_volume":  row[9],
            "taker_buy_quote_asset_volume": row[10],
            "ingested_at":                  ingested_at,
        }
        for row in rows
    ]

    with engine.begin() as conn:
        conn.execute(upsert_sql, records)

    return len(records)

def load_trades(engine, symbol: str, rows: list[dict]) -> int:
    if not rows:
        return 0

    upsert_sql = text("""
        INSERT INTO raw.raw_binance_trades (
            trade_id, symbol, price, qty, quote_qty,
            trade_time, is_buyer_maker, ingested_at
        ) VALUES (
            :trade_id, :symbol, :price, :qty, :quote_qty,
            :trade_time, :is_buyer_maker, :ingested_at
        )
        ON CONFLICT (symbol, trade_id)
        DO NOTHING
    """)

    ingested_at = datetime.now(timezone.utc)
    records = [
        {
            "trade_id":       int(row["id"]),
            "symbol":         symbol,
            "price":          row["price"],
            "qty":            row["qty"],
            "quote_qty":      row["quoteQty"],
            "trade_time":     int(row["time"]),
            "is_buyer_maker": bool(row["isBuyerMaker"]),
            "ingested_at":    ingested_at,
        }
        for row in rows
    ]

    with engine.begin() as conn:
        conn.execute(upsert_sql, records)

    return len(records)

def load_ticker(engine, data: dict) -> int:
    upsert_sql = text("""
        INSERT INTO raw.raw_binance_tickers (
            symbol, price_change, price_change_percent, weighted_avg_price,
            prev_close_price, last_price, last_qty, bid_price, ask_price,
            open_price, high_price, low_price, volume, quote_volume,
            open_time, close_time, trade_count, ingested_at
        ) VALUES (
            :symbol, :price_change, :price_change_percent, :weighted_avg_price,
            :prev_close_price, :last_price, :last_qty, :bid_price, :ask_price,
            :open_price, :high_price, :low_price, :volume, :quote_volume,
            :open_time, :close_time, :trade_count, :ingested_at
        )
        ON CONFLICT (symbol)
        DO UPDATE SET
            price_change          = EXCLUDED.price_change,
            price_change_percent  = EXCLUDED.price_change_percent,
            weighted_avg_price    = EXCLUDED.weighted_avg_price,
            prev_close_price      = EXCLUDED.prev_close_price,
            last_price            = EXCLUDED.last_price,
            last_qty              = EXCLUDED.last_qty,
            bid_price             = EXCLUDED.bid_price,
            ask_price             = EXCLUDED.ask_price,
            open_price            = EXCLUDED.open_price,
            high_price            = EXCLUDED.high_price,
            low_price             = EXCLUDED.low_price,
            volume                = EXCLUDED.volume,
            quote_volume          = EXCLUDED.quote_volume,
            open_time             = EXCLUDED.open_time,
            close_time            = EXCLUDED.close_time,
            trade_count           = EXCLUDED.trade_count,
            ingested_at           = EXCLUDED.ingested_at
    """)

    record = {
        "symbol":               data["symbol"],
        "price_change":         data.get("priceChange"),
        "price_change_percent": data.get("priceChangePercent"),
        "weighted_avg_price":   data.get("weightedAvgPrice"),
        "prev_close_price":     data.get("prevClosePrice"),
        "last_price":           data.get("lastPrice"),
        "last_qty":             data.get("lastQty"),
        "bid_price":            data.get("bidPrice"),
        "ask_price":            data.get("askPrice"),
        "open_price":           data.get("openPrice"),
        "high_price":           data.get("highPrice"),
        "low_price":            data.get("lowPrice"),
        "volume":               data.get("volume"),
        "quote_volume":         data.get("quoteVolume"),
        "open_time":            data.get("openTime"),
        "close_time":           data.get("closeTime"),
        "trade_count":          data.get("count"),
        "ingested_at":          datetime.now(timezone.utc),
    }

    with engine.begin() as conn:
        conn.execute(upsert_sql, record)

    return 1

def ingest_all():
    logger.info("Starting ingestion cycle for symbols: %s", SYMBOLS)
    engine = get_engine()

    for symbol in SYMBOLS:
        symbol = symbol.strip()
        try:
            kline_data = fetch_klines(symbol)
            n_klines = load_klines(engine, symbol, kline_data)
            logger.info("[%s] klines upserted: %d", symbol, n_klines)
        except Exception as exc:
            logger.error("[%s] klines failed: %s", symbol, exc)

        try:
            trade_data = fetch_trades(symbol)
            n_trades = load_trades(engine, symbol, trade_data)
            logger.info("[%s] trades upserted: %d", symbol, n_trades)
        except Exception as exc:
            logger.error("[%s] trades failed: %s", symbol, exc)

        try:
            ticker_data = fetch_ticker(symbol)
            load_ticker(engine, ticker_data)
            logger.info("[%s] ticker upserted", symbol)
        except Exception as exc:
            logger.error("[%s] ticker failed: %s", symbol, exc)

    engine.dispose()
    logger.info("Ingestion cycle complete.")

def main():
    logger.info("Crypto ingestion service starting. Poll interval: %ds", POLL_INTERVAL)
    ingest_all()
    schedule.every(POLL_INTERVAL).seconds.do(ingest_all)
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()
