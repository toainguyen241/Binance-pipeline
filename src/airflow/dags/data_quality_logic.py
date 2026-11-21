import json
from collections.abc import Iterable
from datetime import datetime


def parse_json_each_row(raw: str) -> list[dict]:
    return [json.loads(line) for line in raw.strip().splitlines() if line]


def build_idempotent_alert_insert(alerts: Iterable[dict]) -> str:

    rows = list(alerts)
    if not rows:
        raise ValueError("alerts must not be empty")

    payload = "\n".join(json.dumps(row) for row in rows)
    return (
        "INSERT INTO binance.alerts\n"
        "SELECT alert_type, symbol, detected_at, detail\n"
        "FROM input('alert_type String, symbol String, "
        "detected_at DateTime64(3), detail String')\n"
        "WHERE (alert_type, symbol, detected_at, detail) NOT IN (\n"
        "    SELECT alert_type, symbol, detected_at, detail "
        "FROM binance.alerts\n"
        ")\n"
        "FORMAT JSONEachRow\n" + payload
    )


def build_gap_alerts(gaps: Iterable[dict]) -> list[dict]:
    alerts = []
    for gap in gaps:
        missing = gap["gap_size"] - 1
        alerts.append(
            {
                "alert_type": "gap_detection",
                "symbol": gap["symbol"],
                "detected_at": gap["trade_time"],
                "detail": (
                    f"trade_id nhay tu {gap['prev_trade_id']} len {gap['trade_id']} "
                    f"(thieu {missing} trade)"
                ),
            }
        )
    return alerts


def build_price_anomaly_alerts(anomalies: Iterable[dict]) -> list[dict]:
    alerts = []
    for anomaly in anomalies:
        alerts.append(
            {
                "alert_type": "price_anomaly",
                "symbol": anomaly["symbol"],
                "detected_at": anomaly["window_end"],
                "detail": (
                    f"bien dong {anomaly['pct_range']:.2f}% trong window "
                    f"{anomaly['window_start']} - {anomaly['window_end']} "
                    f"(low={anomaly['low']}, high={anomaly['high']})"
                ),
            }
        )
    return alerts


def build_freshness_alerts(
    rows: Iterable[dict],
    symbols: Iterable[str],
    lookback_minutes: int,
    threshold_seconds: int,
    detected_at: datetime,
) -> list[dict]:
    freshness_by_symbol = {row["symbol"]: row["staleness_seconds"] for row in rows}
    alerts = []

    for symbol in symbols:
        if symbol not in freshness_by_symbol:
            detail = f"khong co trade nao trong {lookback_minutes} phut gan day"
        else:
            staleness = freshness_by_symbol[symbol]
            if staleness <= threshold_seconds:
                continue
            detail = (
                f"trade gan nhat cach day {staleness} giay "
                f"(nguong {threshold_seconds}s)"
            )

        alerts.append(
            {
                "alert_type": "consumer_lag",
                "symbol": symbol,
                "detected_at": detected_at.isoformat(),
                "detail": detail,
            }
        )

    return alerts


TELEGRAM_MAX_ALERT_LINES = 10
TELEGRAM_MAX_MESSAGE_CHARS = 3500


def format_alert_message(
    task_name: str,
    alerts: Iterable[dict],
    max_lines: int = TELEGRAM_MAX_ALERT_LINES,
) -> str:
    rows = list(alerts)
    lines = [f"[Binance DQ] {task_name}: {len(rows)} alert"]
    lines.extend(f"- {row['symbol']}: {row['detail']}" for row in rows[:max_lines])
    if len(rows) > max_lines:
        lines.append(f"... va {len(rows) - max_lines} alert khac")
    return "\n".join(lines)[:TELEGRAM_MAX_MESSAGE_CHARS]
