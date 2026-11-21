-- Additive migration for an existing local ClickHouse volume.
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
INSERT INTO binance.trades_canonical
SELECT *
FROM binance.trades_deduplicated;