from datetime import UTC, datetime, timedelta

from airflow.operators.python import PythonOperator  # type: ignore
from clickhouse_client import query as ch_query
from data_quality_logic import (
    build_freshness_alerts,
    build_gap_alerts,
    build_idempotent_alert_insert,
    build_price_anomaly_alerts,
    parse_json_each_row,
)
from telegram_notifier import notify_telegram_alerts, notify_telegram_on_failure

from airflow import DAG

default_args = {
    "owner": "kai",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": notify_telegram_on_failure,
}
GAP_LOOKBACK_MINUTES = 20
GAP_SETTLING_MINUTES = 5
PRICE_LOOKBACK_MINUTES = 20

SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "BNBUSDT",
    "XRPUSDT",
    "ADAUSDT",
    "DOGEUSDT",
    "AVAXUSDT",
]
FRESHNESS_LOOKBACK_MINUTES = 20
STALENESS_THRESHOLD_SECONDS = 60


def gap_detection(**context):
    sql = f"""
    SELECT symbol, prev_trade_id, trade_id, trade_id - prev_trade_id AS gap_size, trade_time
    FROM (
        SELECT symbol, trade_id, trade_time,
               lagInFrame(trade_id) OVER (PARTITION BY symbol ORDER BY trade_id) AS prev_trade_id
        FROM binance.trades_global_canonical FINAL
        WHERE trade_time >= now() - INTERVAL {GAP_LOOKBACK_MINUTES} MINUTE
          AND trade_time < now() - INTERVAL {GAP_SETTLING_MINUTES} MINUTE
    )
    WHERE prev_trade_id != 0 AND trade_id - prev_trade_id > 1
    ORDER BY symbol, trade_id
    FORMAT JSONEachRow
    """
    raw = ch_query(sql)
    gaps = parse_json_each_row(raw)

    if not gaps:
        print("[gap_detection] Khong phat hien gap nao.")
        return

    insert_rows = build_gap_alerts(gaps)

    ch_query(build_idempotent_alert_insert(insert_rows))
    print(
        f"[gap_detection] Xu ly {len(insert_rows)} alert candidate "
        "bang insert idempotent."
    )
    notify_telegram_alerts("gap_detection", insert_rows)


def price_anomaly(**context):
    sql = f"""
    SELECT symbol, window_start, window_end, low, high, pct_range
    FROM (
        SELECT symbol, window_start, window_end, low, high,
               (high - low) / low * 100 AS pct_range
        FROM binance.trades_ohlc_10s_canonical
        WHERE window_start >= now() - INTERVAL {PRICE_LOOKBACK_MINUTES} MINUTE
          AND low > 0
    )
    WHERE pct_range > 5
    ORDER BY symbol, window_start
    FORMAT JSONEachRow
    """
    raw = ch_query(sql)
    anomalies = parse_json_each_row(raw)

    if not anomalies:
        print("[price_anomaly] Khong phat hien bien dong bat thuong nao.")
        return

    insert_rows = build_price_anomaly_alerts(anomalies)

    ch_query(build_idempotent_alert_insert(insert_rows))
    print(
        f"[price_anomaly] Xu ly {len(insert_rows)} alert candidate "
        "bang insert idempotent."
    )
    notify_telegram_alerts("price_anomaly", insert_rows)


def consumer_lag_check(**context):
    sql = f"""
    SELECT symbol,
           max(trade_time) AS last_trade_time,
           dateDiff('second', max(trade_time), now()) AS staleness_seconds
    FROM binance.trades_global_canonical FINAL
    WHERE trade_time >= now() - INTERVAL {FRESHNESS_LOOKBACK_MINUTES} MINUTE
    GROUP BY symbol
    FORMAT JSONEachRow
    """
    raw = ch_query(sql)
    rows = parse_json_each_row(raw)
    insert_rows = build_freshness_alerts(
        rows=rows,
        symbols=SYMBOLS,
        lookback_minutes=FRESHNESS_LOOKBACK_MINUTES,
        threshold_seconds=STALENESS_THRESHOLD_SECONDS,
        detected_at=datetime.now(UTC).replace(tzinfo=None),
    )

    if not insert_rows:
        print("[consumer_lag_check] Tat ca symbol deu fresh, khong co lag bat thuong.")
        return

    ch_query(build_idempotent_alert_insert(insert_rows))
    print(
        f"[consumer_lag_check] Xu ly {len(insert_rows)} alert candidate "
        "bang insert idempotent."
    )
    notify_telegram_alerts("consumer_lag_check", insert_rows)


with DAG(
    dag_id="binance_data_quality",
    description="Gap detection, price anomaly, consumer lag - chay song song moi 15 phut",
    default_args=default_args,
    schedule=timedelta(minutes=15),
    start_date=datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,
    tags=["binance", "data-quality"],
) as dag:
    task_gap_detection = PythonOperator(
        task_id="gap_detection",
        python_callable=gap_detection,
    )

    task_price_anomaly = PythonOperator(
        task_id="price_anomaly",
        python_callable=price_anomaly,
    )

    task_consumer_lag_check = PythonOperator(
        task_id="consumer_lag_check",
        python_callable=consumer_lag_check,
    )
