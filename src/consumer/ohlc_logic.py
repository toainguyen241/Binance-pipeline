import json
from decimal import Decimal


def parse_trade(raw_value: str) -> dict:
    record = json.loads(raw_value)
    return {
        "symbol": record["s"] if "s" in record else record["symbol"],
        "trade_id": record["t"] if "t" in record else record["trade_id"],
        "price": record["p"] if "p" in record else record["price"],
        "quantity": record["q"] if "q" in record else record["quantity"],
        "trade_time": record["T"] if "T" in record else record["trade_time"],
        "is_buyer_maker": record["m"] if "m" in record else record["is_buyer_maker"],
    }


def create_ohlc_accumulator() -> dict:
    return {
        "open_price": None,
        "open_time": None,
        "open_trade_id": None,
        "close_price": None,
        "close_time": None,
        "close_trade_id": None,
        "high": None,
        "low": None,
        "volume": Decimal("0"),  # noqa: FURB157
        "notional": Decimal("0"),  # noqa: FURB157
        "trade_count": 0,
    }


def add_trade(value: dict, acc: dict) -> dict:
    price = Decimal(value["price"])
    quantity = Decimal(value["quantity"])
    trade_time = value["trade_time"]
    trade_id = value["trade_id"]

    if acc["open_time"] is None or (trade_time, trade_id) < (
        acc["open_time"],
        acc["open_trade_id"],
    ):
        acc["open_price"], acc["open_time"], acc["open_trade_id"] = (
            price,
            trade_time,
            trade_id,
        )
    if acc["close_time"] is None or (trade_time, trade_id) > (
        acc["close_time"],
        acc["close_trade_id"],
    ):
        acc["close_price"], acc["close_time"], acc["close_trade_id"] = (
            price,
            trade_time,
            trade_id,
        )

    acc["high"] = price if acc["high"] is None else max(acc["high"], price)
    acc["low"] = price if acc["low"] is None else min(acc["low"], price)
    acc["volume"] += quantity
    acc["notional"] += price * quantity
    acc["trade_count"] += 1
    return acc


def get_ohlc_result(acc: dict) -> dict:
    vwap = acc["notional"] / acc["volume"] if acc["volume"] else Decimal("0")  # noqa: FURB157
    return {
        "open": acc["open_price"],
        "high": acc["high"],
        "low": acc["low"],
        "close": acc["close_price"],
        "volume": acc["volume"],
        "vwap": vwap,
        "trade_count": acc["trade_count"],
    }


def merge_ohlc_accumulators(acc_a: dict, acc_b: dict) -> dict:
    merged = acc_a.copy()
    if acc_b["open_time"] is not None and (
        merged["open_time"] is None
        or (acc_b["open_time"], acc_b["open_trade_id"])
        < (merged["open_time"], merged["open_trade_id"])
    ):
        merged["open_price"], merged["open_time"], merged["open_trade_id"] = (
            acc_b["open_price"],
            acc_b["open_time"],
            acc_b["open_trade_id"],
        )
    if acc_b["close_time"] is not None and (
        merged["close_time"] is None
        or (acc_b["close_time"], acc_b["close_trade_id"])
        > (merged["close_time"], merged["close_trade_id"])
    ):
        merged["close_price"], merged["close_time"], merged["close_trade_id"] = (
            acc_b["close_price"],
            acc_b["close_time"],
            acc_b["close_trade_id"],
        )
    if acc_b["high"] is not None:
        merged["high"] = (
            acc_b["high"]
            if merged["high"] is None
            else max(merged["high"], acc_b["high"])
        )
    if acc_b["low"] is not None:
        merged["low"] = (
            acc_b["low"] if merged["low"] is None else min(merged["low"], acc_b["low"])
        )
    merged["volume"] += acc_b["volume"]
    merged["notional"] += acc_b["notional"]
    merged["trade_count"] += acc_b["trade_count"]
    return merged
