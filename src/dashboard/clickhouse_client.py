import json
import os
from datetime import datetime, timezone

import requests

CLICKHOUSE_URL = "http://clickhouse:8123/"
CLICKHOUSE_AUTH = (
    os.environ["CLICKHOUSE_USER"],
    os.environ["CLICKHOUSE_PASSWORD"],
)


def _query(sql: str) -> list[dict]:
    response = requests.post(
        CLICKHOUSE_URL,
        params={"query": sql + " FORMAT JSONEachRow"},
        auth=CLICKHOUSE_AUTH,
        timeout=10,
    )
    if response.status_code != 200:
        raise RuntimeError(f"ClickHouse trả lỗi: {response.text}")
    lines = response.text.strip().split("\n")
    return [json.loads(line) for line in lines if line]


def get_latest_prices() -> list[dict]:
    """1 row/symbol — trade mới nhất theo trade_time."""
    sql = """
        SELECT
            symbol,
            price,
            latest_trade_time,
            round(
                greatest(
                    dateDiff('millisecond', latest_trade_time, now64(3)),
                    0
                ) / 1000,
                1
            ) AS staleness_seconds
        FROM
        (
            SELECT
                symbol,
                argMaxMerge(price_state) AS price,
                maxMerge(trade_time_state) AS latest_trade_time
            FROM binance.latest_prices_state
            GROUP BY symbol
        )
    """
    return _query(sql)


def get_pipeline_freshness(symbols: list[str], threshold_seconds: int = 15) -> dict:
    if threshold_seconds <= 0:
        raise ValueError("Freshness threshold khong hop le")
    rows = {row["symbol"]: row for row in get_latest_prices()}
    ages = {
        symbol: float(rows[symbol]["staleness_seconds"]) if symbol in rows else None
        for symbol in symbols
    }
    fresh_symbols = [
        symbol
        for symbol, age in ages.items()
        if age is not None and age < threshold_seconds
    ]
    return {
        "fresh_count": len(fresh_symbols),
        "total_symbols": len(symbols),
        "threshold_seconds": threshold_seconds,
        "max_age_seconds": max(
            (age for age in ages.values() if age is not None), default=None
        ),
        "stale_symbols": [symbol for symbol in symbols if symbol not in fresh_symbols],
    }


def _symbol_list_sql(symbols: list[str]) -> str:
    if not symbols or any(not symbol.isalnum() for symbol in symbols):
        raise ValueError("Danh sách symbol không hợp lệ")
    return ", ".join(f"'{symbol}'" for symbol in symbols)


def get_recent_ohlc(
    symbol: str, limit: int = 2160, lookback_hours: int = 6
) -> list[dict]:

    if not symbol.isalnum():
        raise ValueError("OHLC symbol không hợp lệ")
    if not 1 <= limit <= 5000:
        raise ValueError("OHLC limit không hợp lệ")
    if not 1 <= lookback_hours <= 24:
        raise ValueError("OHLC lookback không hợp lệ")
    sql = f"""
        SELECT
            symbol, window_start, window_end,
            open, high, low, close, volume, vwap, trade_count
        FROM binance.trades_ohlc_10s_canonical
        WHERE symbol = '{symbol}'
          AND window_start >= now() - INTERVAL {lookback_hours} HOUR
        ORDER BY window_start DESC
        LIMIT {limit}
    """
    return _query(sql)


def get_ohlc_1m(symbol: str, limit: int = 1440, lookback_hours: int = 24) -> list[dict]:
    """Aggregate canonical OHLC 10s into one-minute candles for the 24H chart."""
    if not symbol.isalnum():
        raise ValueError("OHLC symbol khong hop le")
    if not 1 <= limit <= 1440:
        raise ValueError("OHLC 1m limit khong hop le")
    if not 1 <= lookback_hours <= 24:
        raise ValueError("OHLC 1m lookback khong hop le")
    sql = f"""
        SELECT
            symbol,
            minute_start AS window_start,
            minute_start + INTERVAL 1 MINUTE AS window_end,
            argMin(source_open, source_window_start) AS open,
            max(source_high) AS high,
            min(source_low) AS low,
            argMax(source_close, source_window_start) AS close,
            sum(source_volume) AS volume,
            if(
                sum(toFloat64(source_volume)) = 0,
                0,
                sum(toFloat64(source_vwap) * toFloat64(source_volume))
                    / sum(toFloat64(source_volume))
            ) AS vwap,
            sum(source_trade_count) AS trade_count
        FROM
        (
            SELECT
                symbol,
                toStartOfMinute(window_start) AS minute_start,
                window_start AS source_window_start,
                open AS source_open,
                high AS source_high,
                low AS source_low,
                close AS source_close,
                volume AS source_volume,
                vwap AS source_vwap,
                trade_count AS source_trade_count
            FROM binance.trades_ohlc_10s_canonical
            WHERE symbol = '{symbol}'
              AND window_start >= now() - INTERVAL {lookback_hours} HOUR
        )
        GROUP BY symbol, minute_start
        ORDER BY minute_start DESC
        LIMIT {limit}
    """
    return _query(sql)


def get_recent_ohlc_all(
    symbols: list[str], limit: int = 50, lookback_minutes: int = 15
) -> list[dict]:
    """Lấy tối đa N final candles/symbol trong một khoảng thời gian hữu hạn."""
    if not 1 <= limit <= 5000:
        raise ValueError("OHLC limit không hợp lệ")
    if not 1 <= lookback_minutes <= 24 * 60:
        raise ValueError("OHLC lookback không hợp lệ")
    sql = f"""
        SELECT
            symbol, window_start, window_end,
            open, high, low, close, volume, vwap, trade_count
        FROM binance.trades_ohlc_10s_canonical
        WHERE symbol IN ({_symbol_list_sql(symbols)})
          AND window_start >= now() - INTERVAL {lookback_minutes} MINUTE
        ORDER BY symbol, window_start DESC
        LIMIT {limit} BY symbol
    """
    return _query(sql)


def get_live_candles(symbols: list[str]) -> list[dict]:
    """OHLC window 10s hiện tại cho mọi symbol trong một time-pruned query."""
    sql = f"""
        SELECT
            symbol,
            toStartOfInterval(now(), INTERVAL 10 SECOND) AS window_start,
            argMin(price, trade_time) AS open,
            max(price) AS high,
            min(price) AS low,
            argMax(price, trade_time) AS close,
            sum(quantity) AS volume,
            count() AS trade_count
        FROM binance.trades_realtime_serving FINAL
        WHERE symbol IN ({_symbol_list_sql(symbols)})
          AND trade_time >= toStartOfInterval(now(), INTERVAL 10 SECOND)
          AND trade_time < toStartOfInterval(now(), INTERVAL 10 SECOND)
                           + INTERVAL 10 SECOND
        GROUP BY symbol
    """
    return _query(sql)


def get_live_candle(symbol: str) -> dict | None:

    rows = get_live_candles([symbol])
    return rows[0] if rows else None


def get_recent_trades(limit: int = 15) -> list[dict]:

    sql = f"""
        SELECT
            symbol,
            trade_id,
            argMax(price, ingested_at) AS price,
            argMax(quantity, ingested_at) AS quantity,
            trade_time
        FROM binance.trades_realtime_serving
        WHERE trade_time >= now() - INTERVAL 15 MINUTE
        GROUP BY symbol, trade_id, trade_time
        ORDER BY trade_time DESC, trade_id DESC
        LIMIT {limit}
    """
    return _query(sql)


def get_data_quality_status(
    symbols: list[str],
    warning_seconds: int = 15,
    stale_seconds: int = 60,
    alert_lookback_minutes: int = 30,
) -> dict:
    """Snapshot freshness hiện tại và các DQ alert gần đây cho dashboard."""
    if warning_seconds < 0 or stale_seconds <= warning_seconds:
        raise ValueError("Ngưỡng freshness không hợp lệ")
    if not 1 <= alert_lookback_minutes <= 24 * 60:
        raise ValueError("Khoảng lookback alert không hợp lệ")

    latest_by_symbol = {row["symbol"]: row for row in get_latest_prices()}
    freshness = []
    for symbol in symbols:
        row = latest_by_symbol.get(symbol)
        age = float(row["staleness_seconds"]) if row else None
        if age is None:
            state = "missing"
        elif age >= stale_seconds:
            state = "stale"
        elif age >= warning_seconds:
            state = "delayed"
        else:
            state = "fresh"
        freshness.append({"symbol": symbol, "staleness_seconds": age, "state": state})

    alerts_sql = f"""
        SELECT alert_type, symbol, detected_at, detail
        FROM binance.alerts_deduplicated
        WHERE detected_at >= now() - INTERVAL {alert_lookback_minutes} MINUTE
        ORDER BY detected_at DESC
        LIMIT 10
    """
    alerts = _query(alerts_sql)
    states = {row["symbol"]: row["state"] for row in freshness}
    return {
        "checked_at": datetime.now(timezone.utc).isoformat(),  # noqa: UP017
        "total_symbols": len(symbols),
        "fresh_count": sum(state == "fresh" for state in states.values()),
        "delayed_symbols": [
            symbol for symbol in symbols if states[symbol] == "delayed"
        ],
        "stale_symbols": [
            symbol for symbol in symbols if states[symbol] in {"stale", "missing"}
        ],
        "freshness": freshness,
        "recent_alert_count": len(alerts),
        "recent_alerts": alerts,
        "alert_lookback_minutes": alert_lookback_minutes,
    }


def get_price_change_24h() -> list[dict]:

    sql = """
        SELECT
            t1.symbol AS symbol,
            t1.price AS current_price,
            t2.reference_price AS reference_price,
            (
                toFloat64(t1.price) - toFloat64(t2.reference_price)
            ) / nullIf(toFloat64(t2.reference_price), 0) * 100 AS change_pct,
            t2.reference_time < (now() - INTERVAL 23 HOUR) AS is_full_24h,
            t3.volume_24h AS volume_24h,
            t3.trade_count AS trade_count,
            t3.low_24h AS low_24h,
            t3.high_24h AS high_24h,
            t3.avg_price_24h AS avg_price_24h
        FROM
        (
            SELECT symbol, argMaxMerge(price_state) AS price
            FROM binance.latest_prices_state
            GROUP BY symbol
        ) AS t1
        INNER JOIN
        (
            SELECT
                symbol,
                argMin(open, window_start) AS reference_price,
                min(window_start) AS reference_time
            FROM binance.trades_ohlc_10s_canonical
            WHERE window_start >= now() - INTERVAL 24 HOUR
            GROUP BY symbol
        ) AS t2
        ON t1.symbol = t2.symbol
        INNER JOIN
        (
            SELECT
                symbol,
                sum(toFloat64(price) * toFloat64(quantity)) AS volume_24h,
                count() AS trade_count,
                min(price) AS low_24h,
                max(price) AS high_24h,
                avg(price) AS avg_price_24h
            FROM binance.trades_realtime_serving FINAL
            WHERE trade_time >= now() - INTERVAL 24 HOUR
            GROUP BY symbol
        ) AS t3
        ON t1.symbol = t3.symbol
    """
    return _query(sql)
