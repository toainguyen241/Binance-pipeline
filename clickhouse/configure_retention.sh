#!/usr/bin/env bash
set -euo pipefail

days="${CLICKHOUSE_RETENTION_DAYS:-7}"
case "$days" in
  ''|*[!0-9]*)
    echo "CLICKHOUSE_RETENTION_DAYS must be an integer" >&2
    exit 1
    ;;
esac
if [ "$days" -lt 2 ] || [ "$days" -gt 365 ]; then
  echo "CLICKHOUSE_RETENTION_DAYS must be between 2 and 365" >&2
  exit 1
fi

clickhouse-client \
  --host clickhouse \
  --user "$CLICKHOUSE_USER" \
  --password "$CLICKHOUSE_PASSWORD" \
  --multiquery <<SQL
ALTER TABLE binance.trades MODIFY TTL trade_time + toIntervalDay($days) DELETE;
ALTER TABLE binance.trades_canonical MODIFY TTL trade_time + toIntervalDay($days) DELETE;
ALTER TABLE binance.trades_global_canonical MODIFY TTL trade_time + toIntervalDay($days) DELETE;
ALTER TABLE binance.trades_realtime_serving MODIFY TTL trade_time + toIntervalDay($days) DELETE;
ALTER TABLE binance.trades_ohlc_10s MODIFY TTL window_start + toIntervalDay($days) DELETE;
ALTER TABLE binance.trades_ohlc_10s_by_engine MODIFY TTL window_start + toIntervalDay($days) DELETE;
ALTER TABLE binance.alerts MODIFY TTL detected_at + toIntervalDay($days) DELETE;
SQL

echo "ClickHouse TTL configured for ${days} days"
