import logging
import os
import shutil
import time
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("runtime_maintenance")

CHECKPOINT_ROOTS = (
    Path("/data/flink-checkpoints"),
    Path("/data/spark-raw-checkpoints"),
    Path("/data/spark-windowed-checkpoints"),
)
AIRFLOW_LOG_ROOT = Path("/data/airflow-logs")
CHECKPOINT_RETENTION_DAYS = int(os.getenv("CHECKPOINT_RETENTION_DAYS", "7"))
AIRFLOW_LOG_RETENTION_DAYS = int(os.getenv("AIRFLOW_LOG_RETENTION_DAYS", "7"))
MAINTENANCE_INTERVAL_SECONDS = int(os.getenv("MAINTENANCE_INTERVAL_SECONDS", "86400"))


def newest_mtime(path: Path) -> float:
    newest = path.stat().st_mtime
    for child in path.rglob("*"):
        if child.is_file() and not child.is_symlink():
            newest = max(newest, child.stat().st_mtime)
    return newest


def prune_checkpoint_root(root: Path, retention_days: int, now: float) -> int:
    if retention_days <= 0 or not root.is_dir():
        return 0
    cutoff = now - retention_days * 86400
    removed = 0
    for child in root.iterdir():
        if not child.is_dir() or child.is_symlink():
            continue
        if newest_mtime(child) < cutoff:
            shutil.rmtree(child)
            removed += 1
    return removed


def prune_old_log_files(root: Path, retention_days: int, now: float) -> int:
    if retention_days <= 0 or not root.is_dir():
        return 0
    cutoff = now - retention_days * 86400
    removed = 0
    for child in root.rglob("*"):
        if child.is_file() and not child.is_symlink() and child.stat().st_mtime < cutoff:
            child.unlink()
            removed += 1
    for directory in sorted(
        (path for path in root.rglob("*") if path.is_dir() and not path.is_symlink()),
        key=lambda path: len(path.parts),
        reverse=True,
    ):
        try:
            directory.rmdir()
        except OSError:
            pass
    return removed


def run_cleanup(now: float | None = None) -> dict[str, int]:
    current_time = time.time() if now is None else now
    checkpoint_directories = sum(
        prune_checkpoint_root(root, CHECKPOINT_RETENTION_DAYS, current_time)
        for root in CHECKPOINT_ROOTS
    )
    airflow_log_files = prune_old_log_files(
        AIRFLOW_LOG_ROOT, AIRFLOW_LOG_RETENTION_DAYS, current_time
    )
    result = {
        "checkpoint_directories": checkpoint_directories,
        "airflow_log_files": airflow_log_files,
    }
    log.info("Retention cleanup completed: %s", result)
    return result


def main() -> None:
    if MAINTENANCE_INTERVAL_SECONDS < 60:
        raise ValueError("MAINTENANCE_INTERVAL_SECONDS must be at least 60")
    while True:
        try:
            run_cleanup()
        except Exception:
            log.exception("Retention cleanup failed")
        time.sleep(MAINTENANCE_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
