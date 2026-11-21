-- Additive dashboard optimization. Existing canonical and engine-aware tables remain unchanged and available for Airflow use.
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
INSERT INTO binance.trades_realtime_serving
SELECT symbol,
    trade_id,
    price,
    quantity,
    trade_time,
    is_buyer_maker,
    ingested_at
FROM binance.trades_global_canonical FINAL
WHERE (trade_time, symbol, trade_id) NOT IN (
        SELECT trade_time,
            symbol,
            trade_id
        FROM binance.trades_realtime_serving FINAL
    );
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
-- Aggregate states are merge-idempotent: replaying this backfill does not change argMax/max results, while new live states merge by symbol.
INSERT INTO binance.latest_prices_state
SELECT symbol,
    argMaxState(price, trade_time) AS price_state,
    maxState(trade_time) AS trade_time_state
FROM binance.trades_global_canonical FINAL
GROUP BY symbol;