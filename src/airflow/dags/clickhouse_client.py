import os

import requests

CLICKHOUSE_URL = "http://clickhouse:8123"
CLICKHOUSE_USER = os.environ["CLICKHOUSE_USER"]
CLICKHOUSE_PASSWORD = os.environ["CLICKHOUSE_PASSWORD"]


def query(sql: str, timeout: int = 10) -> str:

    response = requests.post(
        CLICKHOUSE_URL,
        auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD),
        data=sql.encode("utf-8"),
        timeout=timeout,
    )
    response.raise_for_status()
    return response.text
