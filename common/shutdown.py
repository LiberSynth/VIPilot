"""Кооперативная остановка приложения: прерывание активных батчей без ложных ошибок."""

from __future__ import annotations

import os
import sys
import threading

_shutdown_lock = threading.Lock()
_shutdown_requested = False
_interrupt_count = 0
_batch_threads: dict[str, threading.Thread] = {}


from common.exceptions import ShutdownRequested


def is_shutting_down() -> bool:
    with _shutdown_lock:
        return _shutdown_requested


def request_shutdown(*, force: bool = False) -> None:
    global _shutdown_requested, _interrupt_count
    with _shutdown_lock:
        _interrupt_count += 1
        _shutdown_requested = True
        if force or _interrupt_count >= 2:
            os._exit(130)
    import common.environment as environment

    environment.wakeup_loop()


def register_batch_thread(batch_id: str, thread: threading.Thread) -> None:
    with _shutdown_lock:
        _batch_threads[batch_id] = thread


def unregister_batch_thread(batch_id: str) -> None:
    with _shutdown_lock:
        _batch_threads.pop(batch_id, None)


def wait_for_batch_threads(timeout: float = 120.0) -> None:
    with _shutdown_lock:
        threads = list(_batch_threads.values())
    for thread in threads:
        thread.join(timeout=timeout)
        if thread.is_alive():
            write_log_entry = _write_log_entry()
            write_log_entry(
                None,
                "main",
                f"Таймаут ожидания потока батча {thread.name}.",
                level="warn",
            )


def is_playwright_shutdown_error(exc: BaseException) -> bool:
    msg = str(exc).lower()
    return (
        "has been closed" in msg
        or "target page, context or browser" in msg
        or "browser has been closed" in msg
    )


def graceful_exit(timeout: float = 120.0) -> None:
    """Первое Ctrl+C: дождаться батчей и выйти без KeyboardInterrupt в waitress."""
    import log.log as log_state
    from log import write_log_entry

    with log_state._system_log_lock:
        already = log_state._lifecycle_stop_logged
        if not already:
            log_state._lifecycle_stop_logged = True
    if not already:
        write_log_entry(None, "main", "Остановка приложения, жду завершение батчей.", level="info")
    wait_for_batch_threads(timeout=timeout)
    if not already:
        write_log_entry(None, "main", "Приложение остановлено", level="info")
    sys.exit(0)


def _write_log_entry():
    from log import write_log_entry

    return write_log_entry
