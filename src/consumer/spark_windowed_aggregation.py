import json
import logging
import os
import signal
import time
from threading import Event

from clickhouse_http import post_json_each_row
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    max_by,
    min_by,
    struct,
    window,
)
from pyspark.sql.functions import (
    count as spark_count,
)
from pyspark.sql.functions import (
    max as spark_max,
)
from pyspark.sql.functions import (
    min as spark_min,
)
from pyspark.sql.functions import (
    sum as spark_sum,
)
from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("spark_windowed_aggregation")

KAFKA_BOOTSTRAP = "kafka:19092"
KAFKA_TOPIC = "binance-trades"
CLICKHOUSE_URL = "http://clickhouse:8123/"
CLICKHOUSE_AUTH = (
    os.environ["CLICKHOUSE_USER"],
    os.environ["CLICKHOUSE_PASSWORD"],
)
CLICKHOUSE_INSERT_QUERY = (
    "INSERT INTO binance.trades_ohlc_10s_by_engine FORMAT JSONEachRow"
)
CHECKPOINT_LOCATION = os.getenv(
    "CHECKPOINT_LOCATION", "/tmp/checkpoints/binance_windowed_agg_tuned_v3"
)

WINDOW_DURATION = os.getenv("SPARK_WINDOW_DURATION", "10 seconds")
WATERMARK_DELAY = os.getenv("SPARK_WATERMARK_DELAY", "10 seconds")
TRIGGER_INTERVAL = os.getenv("SPARK_WINDOWED_TRIGGER_INTERVAL", "10 seconds")
OUTPUT_MODE = os.getenv("SPARK_WINDOWED_OUTPUT_MODE", "update").strip().lower()
SHUFFLE_PARTITIONS = int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
MAX_OFFSETS_PER_TRIGGER = int(os.getenv("SPARK_MAX_OFFSETS_PER_TRIGGER", "5000"))
SHUTDOWN_REQUESTED = Event()
GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS = 10

if OUTPUT_MODE not in {"update", "append"}:
    raise ValueError("SPARK_WINDOWED_OUTPUT_MODE must be 'update' or 'append'")

TRADE_SCHEMA = StructType(
    [
        StructField("symbol", StringType()),
        StructField("trade_id", LongType()),
        StructField("price", StringType()),
        StructField("quantity", StringType()),
        StructField("trade_time", LongType()),
        StructField("is_buyer_maker", BooleanType()),
    ]
)


def write_batch_to_clickhouse(batch_df, batch_id: int) -> None:
    rows = batch_df.collect()
    count = len(rows)
    if count == 0:
        log.info("Batch %d rỗng, bỏ qua", batch_id)
        return

    lines = []
    for r in rows:
        lines.append(
            json.dumps(
                {
                    "symbol": r.symbol,
                    "window_start": r.window_start.strftime("%Y-%m-%d %H:%M:%S.%f")[
                        :-3
                    ],
                    "window_end": r.window_end.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                    "open": str(r.open),
                    "high": str(r.high),
                    "low": str(r.low),
                    "close": str(r.close),
                    "volume": str(r.volume),
                    "vwap": str(r.vwap),
                    "trade_count": r.trade_count,
                    "engine": "spark",
                }
            )
        )
    body = "\n".join(lines).encode("utf-8")

    post_json_each_row(
        url=CLICKHOUSE_URL,
        auth=CLICKHOUSE_AUTH,
        query=CLICKHOUSE_INSERT_QUERY,
        body=body,
        logger=log,
    )
    log.info("Batch %d: ghi %d window row vào ClickHouse", batch_id, count)


def request_shutdown(signum: int, _frame: object) -> None:
    log.info("Nhận signal %s, chờ batch hiện tại và dừng Spark query", signum)
    SHUTDOWN_REQUESTED.set()


def stop_query_after_active_batch(query: object) -> None:
    deadline = time.monotonic() + GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS
    while (
        query.isActive  # type: ignore
        and query.status.get("isTriggerActive", False)  # type: ignore
        and time.monotonic() < deadline
    ):
        time.sleep(0.05)
    if query.isActive:  # type: ignore
        query.stop()  # type: ignore


def main() -> None:
    signal.signal(signal.SIGTERM, request_shutdown)
    signal.signal(signal.SIGINT, request_shutdown)
    spark = (
        SparkSession.builder.appName("binance-windowed-ohlc")  # type: ignore
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(SHUFFLE_PARTITIONS))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info(
        (
            "Spark OHLC config: window=%s watermark=%s trigger=%s "
            "output_mode=%s shuffle_partitions=%s max_offsets=%s"
        ),
        WINDOW_DURATION,
        WATERMARK_DELAY,
        TRIGGER_INTERVAL,
        OUTPUT_MODE,
        SHUFFLE_PARTITIONS,
        MAX_OFFSETS_PER_TRIGGER,
    )

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .load()
    )

    parsed = (
        raw_stream.selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), TRADE_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("price", col("price").cast("decimal(18,8)"))
        .withColumn("quantity", col("quantity").cast("decimal(18,8)"))
        .withColumn("trade_time", (col("trade_time") / 1000).cast("timestamp"))
        .withColumn("notional", col("price") * col("quantity"))
    )

    windowed = (
        parsed.withWatermark("trade_time", WATERMARK_DELAY)
        .groupBy(col("symbol"), window(col("trade_time"), WINDOW_DURATION))
        .agg(
            min_by("price", struct("trade_time", "trade_id")).alias("open"),
            spark_max("price").alias("high"),
            spark_min("price").alias("low"),
            max_by("price", struct("trade_time", "trade_id")).alias("close"),
            spark_sum("quantity").alias("volume"),
            (spark_sum("notional") / spark_sum("quantity")).alias("vwap"),
            spark_count("*").alias("trade_count"),
        )
        .select(
            col("symbol"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "open",
            "high",
            "low",
            "close",
            "volume",
            "vwap",
            "trade_count",
        )
    )

    query = (
        windowed.writeStream.outputMode(OUTPUT_MODE)
        .foreachBatch(write_batch_to_clickhouse)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .queryName("binance-spark-ohlc")
        .start()
    )
    try:
        while not SHUTDOWN_REQUESTED.is_set() and query.isActive:
            query.awaitTermination(1)
    finally:
        stop_query_after_active_batch(query)
        spark.stop()
        log.info("Spark OHLC query đã dừng an toàn")


if __name__ == "__main__":
    main()
