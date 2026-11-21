import requests
from airflow.models import Variable

TELEGRAM_API_URL = "https://api.telegram.org/bot{token}/sendMessage"
TELEGRAM_TIMEOUT_SECONDS = 10


def _get_credentials() -> tuple[str, str] | None:
    """Return (token, chat_id), or None when Telegram is not configured."""
    try:
        token = Variable.get("telegram_bot_token")
        chat_id = Variable.get("telegram_chat_id")
    except KeyError:
        return None
    if not token or not chat_id:
        return None
    return token, chat_id


def _send_message(text: str) -> None:
    credentials = _get_credentials()
    if credentials is None:
        print(
            "[telegram_notifier] Chua set Airflow Variable "
            "telegram_bot_token/telegram_chat_id, bo qua alert."
        )
        return
    token, chat_id = credentials

    try:
        response = requests.post(
            TELEGRAM_API_URL.format(token=token),
            data={"chat_id": chat_id, "text": text},
            timeout=TELEGRAM_TIMEOUT_SECONDS,
        )
    except requests.RequestException as exc:
        print(f"[telegram_notifier] Gui Telegram that bai: {type(exc).__name__}")
        return

    if response.ok:
        print(f"[telegram_notifier] Telegram response: {response.status_code}")
    else:
        print(
            f"[telegram_notifier] Telegram response: {response.status_code} "
            f"{response.text}"
        )


def notify_telegram_on_failure(context):
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    execution_date = context["execution_date"]
    exception = context.get("exception")

    _send_message(
        f"[Airflow FAILED]\n"
        f"DAG: {dag_id}\n"
        f"Task: {task_id}\n"
        f"Time: {execution_date}\n"
        f"Error: {exception}"
    )


def notify_telegram_alerts(task_name: str, alerts: list[dict]) -> None:
    """Best-effort notification for data-quality findings."""
    if not alerts:
        return
    try:
        from data_quality_logic import format_alert_message

        _send_message(format_alert_message(task_name, alerts))
    except Exception as exc:  # noqa: BLE001
        print(f"[telegram_notifier] Bo qua notify DQ: {type(exc).__name__}")
