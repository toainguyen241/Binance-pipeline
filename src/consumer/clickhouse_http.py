import logging
import time

import requests

DEFAULT_MAX_ATTEMPTS = 5
DEFAULT_INITIAL_DELAY_SECONDS = 1
DEFAULT_MAX_DELAY_SECONDS = 8


def post_json_each_row(
    *,
    url: str,
    auth: tuple[str, str],
    query: str,
    body: bytes,
    logger: logging.Logger,
    timeout: int = 10,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
) -> None:

    delay = DEFAULT_INITIAL_DELAY_SECONDS

    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.post(
                url,
                params={"query": query},
                data=body,
                auth=auth,
                timeout=timeout,
            )
            response.raise_for_status()
            return
        except requests.RequestException as exc:
            response = getattr(exc, "response", None)
            status_code = response.status_code if response is not None else None
            retryable_status = status_code in {408, 429} or (
                status_code is not None and status_code >= 500
            )
            if status_code is not None and not retryable_status:
                logger.error(
                    "ClickHouse từ chối request với HTTP %d; không retry lỗi vĩnh viễn.",
                    status_code,
                )
                raise
            if attempt == max_attempts:
                logger.error(
                    "Ghi ClickHouse thất bại sau %d lần thử: %s",
                    max_attempts,
                    exc,
                )
                raise

            logger.warning(
                "Ghi ClickHouse thất bại lần %d/%d (%s), thử lại sau %d giây.",
                attempt,
                max_attempts,
                exc,
                delay,
            )
            time.sleep(delay)
            delay = min(delay * 2, DEFAULT_MAX_DELAY_SECONDS)
