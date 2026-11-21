-- Additive, non-destructive migration for an existing local ClickHouse volume.
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
-- Also fixes the default if an earlier draft of this additive migration was applied before consumers started writing to the new table.
ALTER TABLE binance.trades_ohlc_10s_by_engine
MODIFY COLUMN updated_at DateTime64(3) DEFAULT now64(3);
INSERT INTO binance.trades_ohlc_10s_by_engine
SELECT symbol,
    window_start,
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
FROM binance.trades_ohlc_10s
WHERE (engine, symbol, window_start) NOT IN (
        SELECT engine,
            symbol,
            window_start
        FROM binance.trades_ohlc_10s_by_engine
    );
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