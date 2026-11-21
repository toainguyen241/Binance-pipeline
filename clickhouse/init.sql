CREATE DATABASE IF NOT EXISTS binance;
CREATE TABLE IF NOT EXISTS binance.trades (
    symbol LowCardinality(String),
    trade_id UInt64,
    price Decimal64(8),
    quantity Decimal64(8),
    trade_time DateTime64(3),
    is_buyer_maker Bool,
    engine LowCardinality(String) DEFAULT 'unknown',
    ingested_at DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree()
ORDER BY (symbol, trade_time);
-- Canonical read model: HTTP sinks are at-least-once, therefore retries can  append the same trade more than once.
-- Keep the raw table append-only and collapse duplicates by the stable business key for Dashboard/Airflow reads.
CREATE VIEW IF NOT EXISTS binance.trades_deduplicated AS
SELECT symbol,
    trade_id,
    tupleElement(first_seen, 1) AS price,
    tupleElement(first_seen, 2) AS quantity,
    tupleElement(first_seen, 3) AS trade_time,
    tupleElement(first_seen, 4) AS is_buyer_maker,
    engine,
    tupleElement(first_seen, 5) AS ingested_at
FROM (
        SELECT engine,
            symbol,
            trade_id,
            argMin(
                tuple(
                    price,
                    quantity,
                    trade_time,
                    is_buyer_maker,
                    ingested_at
                ),
                ingested_at
            ) AS first_seen
        FROM binance.trades
        GROUP BY engine,
            symbol,
            trade_id
    );
-- Serving table optimized for idempotent reads. The raw table remains the immutable ingestion log; this copy collapses retries by business key.
CREATE TABLE IF NOT EXISTS binance.trades_canonical (
    symbol LowCardinality(String),
    trade_id UInt64,
    price Decimal64(8),
    quantity Decimal64(8),
    trade_time DateTime64(3),
    is_buyer_maker Bool,
    engine LowCardinality(String),
    ingested_at DateTime64(3)
) ENGINE = ReplacingMergeTree(ingested_at) PARTITION BY toYYYYMM(trade_time)
ORDER BY (engine, symbol, trade_id);
CREATE MATERIALIZED VIEW IF NOT EXISTS binance.trades_canonical_mv TO binance.trades_canonical AS
SELECT symbol,
    trade_id,
    price,
    quantity,
    trade_time,
    is_buyer_maker,
    engine,
    ingested_at
FROM binance.trades;
-- Engine-neutral serving model for Dashboard and Airflow. The engine-aware canonical table above remains the source for per-engine comparisons,
-- while this table collapses the same Binance trade across retries and engines by the market business key (symbol, trade_id).
CREATE TABLE IF NOT EXISTS binance.trades_global_canonical (
    symbol LowCardinality(String),
    trade_id UInt64,
    price Decimal64(8),
    quantity Decimal64(8),
    trade_time DateTime64(3),
    is_buyer_maker Bool,
    ingested_at DateTime64(3)
) ENGINE = ReplacingMergeTree(ingested_at) PARTITION BY toYYYYMM(trade_time)
ORDER BY (symbol, trade_id);
CREATE MATERIALIZED VIEW IF NOT EXISTS binance.trades_global_canonical_mv TO binance.trades_global_canonical AS
SELECT symbol,
    trade_id,
    price,
    quantity,
    trade_time,
    is_buyer_maker,
    ingested_at
FROM binance.trades;
-- Dashboard-optimized read model. It keeps the same global dedup semantics as trades_global_canonical but begins the primary key with trade_time so recent trades and current-window candles do not scan the full history.
CREATE TABLE IF NOT EXISTS binance.trades_realtime_serving (
    symbol LowCardinality(String),
    trade_id UInt64,
    price Decimal64(8),
    quantity Decimal64(8),
    trade_time DateTime64(3),
    is_buyer_maker Bool,
    ingested_at DateTime64(3)
) ENGINE = ReplacingMergeTree(ingested_at) PARTITION BY toYYYYMM(trade_time)
ORDER BY (trade_time, symbol, trade_id);
CREATE MATERIALIZED VIEW IF NOT EXISTS binance.trades_realtime_serving_mv TO binance.trades_realtime_serving AS
SELECT symbol,
    trade_id,
    price,
    quantity,
    trade_time,
    is_buyer_maker,
    ingested_at
FROM binance.trades;
-- Compact latest-price state: ticker refreshes no longer aggregate millions of canonical rows every 0.5 seconds.
CREATE TABLE IF NOT EXISTS binance.latest_prices_state (
    symbol LowCardinality(String),
    price_state AggregateFunction(argMax, Decimal64(8), DateTime64(3)),
    trade_time_state AggregateFunction(max, DateTime64(3))
) ENGINE = AggregatingMergeTree()
ORDER BY symbol;
CREATE MATERIALIZED VIEW IF NOT EXISTS binance.latest_prices_state_mv TO binance.latest_prices_state AS
SELECT symbol,
    argMaxState(price, trade_time) AS price_state,
    maxState(trade_time) AS trade_time_state
FROM binance.trades
GROUP BY symbol;
CREATE TABLE IF NOT EXISTS binance.trades_ohlc_10s (
    symbol LowCardinality(String),
    window_start DateTime64(3),
    window_end DateTime64(3),
    open Decimal64(8),
    high Decimal64(8),
    low Decimal64(8),
    close Decimal64(8),
    volume Decimal64(8),
    vwap Decimal64(8),
    trade_count UInt32,
    engine LowCardinality(String) DEFAULT 'unknown',
    updated_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (symbol, window_start);
-- Engine-aware OHLC storage. The legacy table above intentionally remains so existing local volumes can be migrated without dropping or renaming data.
CREATE TABLE IF NOT EXISTS binance.trades_ohlc_10s_by_engine (
    symbol LowCardinality(String),
    window_start DateTime64(3),
    window_end DateTime64(3),
    open Decimal64(8),
    high Decimal64(8),
    low Decimal64(8),
    close Decimal64(8),
    volume Decimal64(8),
    vwap Decimal64(8),
    trade_count UInt32,
    engine LowCardinality(String),
    updated_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at) PARTITION BY toYYYYMM(window_start)
ORDER BY (engine, symbol, window_start);
-- Canonical chart/data-quality view: one latest engine result per market window. Engine comparison queries read the physical by-engine table instead.
CREATE VIEW IF NOT EXISTS binance.trades_ohlc_10s_canonical AS
SELECT symbol,
    window_start,
    tupleElement(latest, 1) AS window_end,
    tupleElement(latest, 2) AS open,
    tupleElement(latest, 3) AS high,
    tupleElement(latest, 4) AS low,
    tupleElement(latest, 5) AS close,
    tupleElement(latest, 6) AS volume,
    tupleElement(latest, 7) AS vwap,
    tupleElement(latest, 8) AS trade_count,
    tupleElement(latest, 9) AS engine,
    tupleElement(latest, 10) AS updated_at
FROM (
        SELECT symbol,
            window_start,
            argMax(
                tuple(
                    window_end,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    vwap,
                    trade_count,
                    engine,
                    updated_at
                ),
                updated_at
            ) AS latest
        FROM binance.trades_ohlc_10s_by_engine FINAL
        GROUP BY symbol,
            window_start
    );
CREATE TABLE IF NOT EXISTS binance.alerts (
    alert_type LowCardinality(String),
    symbol LowCardinality(String),
    detected_at DateTime64(3),
    detail String
) ENGINE = MergeTree()
ORDER BY (alert_type, detected_at);
CREATE VIEW IF NOT EXISTS binance.alerts_deduplicated AS
SELECT DISTINCT alert_type,
    symbol,
    detected_at,
    detail
FROM binance.alerts;