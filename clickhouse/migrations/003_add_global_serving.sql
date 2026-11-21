-- Additive, non-destructive engine-neutral serving model for Dashboard and Airflow.
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
INSERT INTO binance.trades_global_canonical
SELECT symbol,
    trade_id,
    tupleElement(latest, 1) AS price,
    tupleElement(latest, 2) AS quantity,
    tupleElement(latest, 3) AS trade_time,
    tupleElement(latest, 4) AS is_buyer_maker,
    tupleElement(latest, 5) AS ingested_at
FROM (
        SELECT symbol,
            trade_id,
            argMax(
                tuple(
                    price,
                    quantity,
                    trade_time,
                    is_buyer_maker,
                    ingested_at
                ),
                ingested_at
            ) AS latest
        FROM binance.trades_canonical FINAL
        GROUP BY symbol,
            trade_id
    ) AS source
WHERE (symbol, trade_id) NOT IN (
        SELECT symbol,
            trade_id
        FROM binance.trades_global_canonical FINAL
    );