#!/usr/bin/env bash
set -u

"$@" &
spark_pid=$!
shutdown_requested=0
watchdog_pid=""

graceful_shutdown() {
    if [ "$shutdown_requested" -eq 1 ]; then
        return
    fi
    shutdown_requested=1

    python_pid="$(
        ps -eo pid=,ppid=,comm= |
            awk -v parent="$spark_pid" '$2 == parent && $3 ~ /^python/ { print $1; exit }'
    )"
    if [ -n "$python_pid" ]; then
        echo "Requesting graceful Spark query shutdown (python pid $python_pid)"
        kill -TERM "$python_pid" 2>/dev/null || true
    else
        echo "Python driver not ready; forwarding shutdown to spark-submit"
        kill -TERM "$spark_pid" 2>/dev/null || true
    fi

    (
        sleep 20
        if kill -0 "$spark_pid" 2>/dev/null; then
            echo "Grace period elapsed; terminating spark-submit"
            kill -TERM "$spark_pid" 2>/dev/null || true
        fi
    ) &
    watchdog_pid=$!
}

trap graceful_shutdown TERM INT

exit_code=0
while kill -0 "$spark_pid" 2>/dev/null; do
    wait "$spark_pid" || exit_code=$?
done

if [ -n "$watchdog_pid" ]; then
    kill "$watchdog_pid" 2>/dev/null || true
fi

if [ "$shutdown_requested" -eq 1 ]; then
    exit 0
fi
exit "$exit_code"
