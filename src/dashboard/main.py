import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from src.dashboard.clickhouse_client import (
    get_data_quality_status,
    get_latest_prices,
    get_live_candles,
    get_ohlc_1m,
    get_pipeline_freshness,
    get_price_change_24h,
    get_recent_ohlc,
    get_recent_ohlc_all,
    get_recent_trades,
)
from src.dashboard.pipeline_health import pipeline_health_monitor

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

LATEST_PRICE_INTERVAL_SECONDS = 0.5
OHLC_INTERVAL_SECONDS = 2
LIVE_CANDLE_INTERVAL_SECONDS = 0.5
RECENT_TRADES_INTERVAL_SECONDS = 1
PRICE_CHANGE_INTERVAL_SECONDS = 30
DATA_QUALITY_INTERVAL_SECONDS = 15
IDLE_POLL_INTERVAL_SECONDS = 0.5
WEBSOCKET_SEND_TIMEOUT_SECONDS = 2
PIPELINE_FRESHNESS_THRESHOLD_SECONDS = int(
    os.getenv("PIPELINE_FRESHNESS_THRESHOLD_SECONDS", "60")
)
OHLC_TIMEFRAMES = {
    "1H": {"limit": 360, "lookback_hours": 1, "resolution": "10s"},
    "3H": {"limit": 1080, "lookback_hours": 3, "resolution": "10s"},
    "6H": {"limit": 2160, "lookback_hours": 6, "resolution": "10s"},
    "24H": {"limit": 1440, "lookback_hours": 24, "resolution": "1m"},
}


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("dashboard")


@asynccontextmanager
async def lifespan(_app: FastAPI):
    await start_background_tasks()
    try:
        yield
    finally:
        await stop_background_tasks()


app = FastAPI(title="Binance ETL Dashboard", lifespan=lifespan)

app.mount("/static", StaticFiles(directory="src/dashboard/static"), name="static")


class ConnectionManager:
    def __init__(self) -> None:
        self.active_connections: set[WebSocket] = set()

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self.active_connections.add(websocket)
        log.info("Client connected, tổng %d client", len(self.active_connections))

    def disconnect(self, websocket: WebSocket) -> None:
        self.active_connections.discard(websocket)
        log.info("Client disconnected, còn %d client", len(self.active_connections))

    async def broadcast(self, message: str) -> None:
        async def send(connection: WebSocket) -> WebSocket | None:
            try:
                await asyncio.wait_for(
                    connection.send_text(message),
                    timeout=WEBSOCKET_SEND_TIMEOUT_SECONDS,
                )
                return None
            except Exception:  # noqa: BLE001
                return connection

        connections = tuple(self.active_connections)
        dead_connections = await asyncio.gather(*(send(c) for c in connections))
        for dead in dead_connections:
            if dead is None:
                continue
            self.active_connections.discard(dead)


manager = ConnectionManager()
background_tasks: dict[str, asyncio.Task[None]] = {}
background_task_errors: set[str] = set()


def mark_background_success(name: str) -> None:
    background_task_errors.discard(name)


def mark_background_error(name: str) -> None:
    background_task_errors.add(name)


async def broadcast_latest_price_loop() -> None:
    while True:
        if not manager.active_connections:
            await asyncio.sleep(IDLE_POLL_INTERVAL_SECONDS)
            continue
        try:
            data = await asyncio.to_thread(get_latest_prices)
            await manager.broadcast(json.dumps({"type": "latest_price", "data": data}))
            mark_background_success("latest_price")
        except Exception:
            mark_background_error("latest_price")
            log.exception("Lỗi khi poll latest_price")
        await asyncio.sleep(LATEST_PRICE_INTERVAL_SECONDS)


async def broadcast_recent_trades_loop() -> None:
    while True:
        if not manager.active_connections:
            await asyncio.sleep(IDLE_POLL_INTERVAL_SECONDS)
            continue
        try:
            data = await asyncio.to_thread(get_recent_trades, 15)
            await manager.broadcast(json.dumps({"type": "recent_trades", "data": data}))
            mark_background_success("recent_trades")
        except Exception:
            mark_background_error("recent_trades")
            log.exception("Lỗi khi poll recent_trades")
        await asyncio.sleep(RECENT_TRADES_INTERVAL_SECONDS)


async def broadcast_ohlc_loop() -> None:
    while True:
        if not manager.active_connections:
            await asyncio.sleep(IDLE_POLL_INTERVAL_SECONDS)
            continue
        try:
            rows = await asyncio.to_thread(get_recent_ohlc_all, SYMBOLS, 50)
            grouped = {symbol: [] for symbol in SYMBOLS}
            for row in rows:
                grouped[row["symbol"]].append(row)
            for symbol, data in grouped.items():
                await manager.broadcast(
                    json.dumps({"type": "ohlc", "symbol": symbol, "data": data})
                )
            mark_background_success("ohlc")
        except Exception:
            mark_background_error("ohlc")
            log.exception("Lỗi khi poll ohlc")
        await asyncio.sleep(OHLC_INTERVAL_SECONDS)


async def broadcast_live_candle_loop() -> None:
    while True:
        if not manager.active_connections:
            await asyncio.sleep(IDLE_POLL_INTERVAL_SECONDS)
            continue
        try:
            candles = await asyncio.to_thread(get_live_candles, SYMBOLS)
            for candle in candles:
                await manager.broadcast(
                    json.dumps(
                        {
                            "type": "live_candle",
                            "symbol": candle["symbol"],
                            "data": candle,
                        }
                    )
                )
            mark_background_success("live_candle")
        except Exception:
            mark_background_error("live_candle")
            log.exception("Lỗi khi poll live_candle")
        await asyncio.sleep(LIVE_CANDLE_INTERVAL_SECONDS)


async def broadcast_price_change_loop() -> None:
    while True:
        if not manager.active_connections:
            await asyncio.sleep(IDLE_POLL_INTERVAL_SECONDS)
            continue
        try:
            data = await asyncio.to_thread(get_price_change_24h)
            await manager.broadcast(json.dumps({"type": "price_change", "data": data}))
            mark_background_success("price_change")
        except Exception:
            mark_background_error("price_change")
            log.exception("Lỗi khi poll price_change")
        await asyncio.sleep(PRICE_CHANGE_INTERVAL_SECONDS)


async def broadcast_data_quality_loop() -> None:
    while True:
        if not manager.active_connections:
            await asyncio.sleep(IDLE_POLL_INTERVAL_SECONDS)
            continue
        try:
            data = await asyncio.to_thread(get_data_quality_status, SYMBOLS)
            await manager.broadcast(json.dumps({"type": "data_quality", "data": data}))
            mark_background_success("data_quality")
        except Exception:
            mark_background_error("data_quality")
            log.exception("Lỗi khi poll data_quality")
        await asyncio.sleep(DATA_QUALITY_INTERVAL_SECONDS)


async def supervise_background_loop(name: str, loop_factory) -> None:

    while True:
        try:
            await loop_factory()
            mark_background_error(name)
            log.error("Background task %s stopped unexpectedly; restarting", name)
        except asyncio.CancelledError:
            raise
        except Exception:
            mark_background_error(name)
            log.exception("Background task %s crashed; restarting", name)
        await asyncio.sleep(1)


BACKGROUND_TASK_FACTORIES = {
    "latest_price": broadcast_latest_price_loop,
    "ohlc": broadcast_ohlc_loop,
    "live_candle": broadcast_live_candle_loop,
    "recent_trades": broadcast_recent_trades_loop,
    "price_change": broadcast_price_change_loop,
    "data_quality": broadcast_data_quality_loop,
}


async def start_background_tasks() -> None:
    for name, loop_factory in BACKGROUND_TASK_FACTORIES.items():
        existing = background_tasks.get(name)
        if existing is not None and not existing.done():
            continue
        background_tasks[name] = asyncio.create_task(
            supervise_background_loop(name, loop_factory),
            name=f"dashboard:{name}",
        )


async def stop_background_tasks() -> None:
    tasks = list(background_tasks.values())
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    background_tasks.clear()
    background_task_errors.clear()


@app.get("/health")
def health() -> dict:
    task_status = {
        name: (
            "stopped"
            if (task := background_tasks.get(name)) is None or task.done()
            else "error"
            if name in background_task_errors
            else "running"
        )
        for name in BACKGROUND_TASK_FACTORIES
    }
    errors = []
    try:
        freshness = get_pipeline_freshness(
            SYMBOLS, PIPELINE_FRESHNESS_THRESHOLD_SECONDS
        )
    except Exception as exc:  # noqa: BLE001
        freshness = {"error": type(exc).__name__}
        errors.append("clickhouse_freshness")
    try:
        kafka = pipeline_health_monitor.snapshot()
    except Exception as exc:  # noqa: BLE001
        kafka = {"error": type(exc).__name__}
        errors.append("kafka")

    tasks_ok = all(state == "running" for state in task_status.values())
    freshness_ok = freshness.get("fresh_count") == len(SYMBOLS)
    progress_ok = kafka.get("progress", {}).get("state") in {  # type: ignore
        "warming_up",
        "progressing",
    }
    lag_ok = kafka.get("lag_state") in {"ok", "not_applicable"}
    return {
        "status": (
            "ok"
            if tasks_ok and freshness_ok and progress_ok and lag_ok and not errors
            else "degraded"
        ),
        "background_tasks": task_status,
        "freshness": freshness,
        "kafka": kafka,
        "errors": errors,
    }


@app.get("/")
def index() -> FileResponse:
    return FileResponse("src/dashboard/static/index.html")


@app.get("/api/ohlc/{symbol}")
def api_ohlc(symbol: str, timeframe: str = "6H") -> list[dict]:
    if symbol not in SYMBOLS:
        return []
    timeframe = timeframe.upper()
    config = OHLC_TIMEFRAMES.get(timeframe)
    if config is None:
        return []
    if config["resolution"] == "1m":
        return get_ohlc_1m(
            symbol,
            limit=config["limit"],
            lookback_hours=config["lookback_hours"],
        )
    return get_recent_ohlc(
        symbol,
        limit=config["limit"],
        lookback_hours=config["lookback_hours"],
    )


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
