CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;

SET search_path TO raw;

CREATE TABLE IF NOT EXISTS raw_binance_klines (
    symbol                      VARCHAR(20)     NOT NULL,
    open_time                   BIGINT          NOT NULL,
    open                        NUMERIC(24, 8)  NOT NULL,
    high                        NUMERIC(24, 8)  NOT NULL,
    low                         NUMERIC(24, 8)  NOT NULL,
    close                       NUMERIC(24, 8)  NOT NULL,
    volume                      NUMERIC(36, 8)  NOT NULL,
    close_time                  BIGINT          NOT NULL,
    quote_asset_volume          NUMERIC(36, 8)  NOT NULL,
    number_of_trades            BIGINT          NOT NULL,
    taker_buy_base_asset_volume NUMERIC(36, 8)  NOT NULL,
    taker_buy_quote_asset_volume NUMERIC(36, 8) NOT NULL,
    ingested_at                 TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_klines_symbol_opentime UNIQUE (symbol, open_time)
);

CREATE TABLE IF NOT EXISTS raw_binance_trades (
    trade_id       BIGINT          NOT NULL,
    symbol         VARCHAR(20)     NOT NULL,
    price          NUMERIC(24, 8)  NOT NULL,
    qty            NUMERIC(36, 8)  NOT NULL,
    quote_qty      NUMERIC(36, 8)  NOT NULL,
    trade_time     BIGINT          NOT NULL,
    is_buyer_maker BOOLEAN         NOT NULL,
    ingested_at    TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_trades_symbol_tradeid UNIQUE (symbol, trade_id)
);

CREATE TABLE IF NOT EXISTS raw_binance_tickers (
    symbol                  VARCHAR(20)     NOT NULL,
    price_change            NUMERIC(24, 8),
    price_change_percent    NUMERIC(10, 4),
    weighted_avg_price      NUMERIC(24, 8),
    prev_close_price        NUMERIC(24, 8),
    last_price              NUMERIC(24, 8)  NOT NULL,
    last_qty                NUMERIC(36, 8),
    bid_price               NUMERIC(24, 8),
    ask_price               NUMERIC(24, 8),
    open_price              NUMERIC(24, 8),
    high_price              NUMERIC(24, 8),
    low_price               NUMERIC(24, 8),
    volume                  NUMERIC(36, 8),
    quote_volume            NUMERIC(36, 8),
    open_time               BIGINT,
    close_time              BIGINT,
    trade_count             BIGINT,
    ingested_at             TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_tickers_symbol UNIQUE (symbol)
);

CREATE INDEX IF NOT EXISTS idx_klines_symbol ON raw_binance_klines (symbol);
CREATE INDEX IF NOT EXISTS idx_klines_open_time ON raw_binance_klines (open_time DESC);
CREATE INDEX IF NOT EXISTS idx_trades_symbol ON raw_binance_trades (symbol);
CREATE INDEX IF NOT EXISTS idx_trades_trade_time ON raw_binance_trades (trade_time DESC);
CREATE INDEX IF NOT EXISTS idx_tickers_symbol ON raw_binance_tickers (symbol);