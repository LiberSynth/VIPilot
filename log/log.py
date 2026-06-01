"""
Все функции логирования приложения.
"""

import sys
import threading

_ALLOWED_CATEGORIES = {
    "api",
    "planning",
    "story",
    "video",
    "transcode",
    "publish",
    "system",
}

_ALLOWED_LOG_LEVELS = {
    "silent",
    "info",
    "warn",
    "error",
    "fatal_error",
}

_lifecycle_stop_logged = False
_system_log_id: str | None = None
_system_log_lock = threading.Lock()


def _stdout_log(message: str) -> None:
    text = message.encode("cp1251", errors="replace").decode("cp1251")
    try:
        print(text)
    except UnicodeEncodeError:
        sys.stdout.buffer.write(text.encode("cp1251", errors="replace") + b"\n")
        sys.stdout.flush()


def log_app_started():
    global _lifecycle_stop_logged, _system_log_id
    with _system_log_lock:
        _lifecycle_stop_logged = False
        _system_log_id = None
    write_log_entry(None, "system", "[main] Приложение запущено", level="info")


def log_app_stopped():
    global _lifecycle_stop_logged
    if _lifecycle_stop_logged:
        return
    _lifecycle_stop_logged = True
    write_log_entry(None, "system", "[main] Приложение остановлено", level="info")


def write_log_entry(batch_id, category, message, level="info"):
    """Единая точка записи в log и log_entries.

    batch_id=None — системное окно между батчами (system_log_id), category='system'.
    batch_id задан — пишет в log (batch_id, category); system_log_id сбрасывается
    только при создании первой строки log для этого batch_id.
    """
    import common.environment as environment
    from db.db_simple import db_get_or_create_log, db_insert_log, db_insert_log_entry

    global _system_log_id

    assert category in _ALLOWED_CATEGORIES, (
        f"write_log_entry: недопустимая category {category!r}. "
        f"Допустимые: {_ALLOWED_CATEGORIES}."
    )
    assert level in _ALLOWED_LOG_LEVELS, (
        f"write_log_entry: недопустимый уровень {level!r}. "
        f"Допустимые: {_ALLOWED_LOG_LEVELS}."
    )
    if batch_id is None:
        assert category == "system", (
            f"write_log_entry: при batch_id=None category должна быть 'system', "
            f"получено {category!r}."
        )

    _stdout_log(message)
    if level == "silent" and not environment.deep_debugging:
        return None

    mod_token = environment.asserted_log_modification.set(True)
    entry_token = environment.asserted_log_entry.set(True)
    try:
        with _system_log_lock:
            if batch_id is not None:
                log_id, is_first_batch_log = db_get_or_create_log(batch_id, category)
                if is_first_batch_log:
                    _system_log_id = None
                db_insert_log_entry(log_id, message, level)
            else:
                if _system_log_id is None:
                    _system_log_id = db_insert_log(None, "system")
                db_insert_log_entry(_system_log_id, message, level)
                log_id = _system_log_id
    finally:
        environment.asserted_log_entry.reset(entry_token)
        environment.asserted_log_modification.reset(mod_token)

    if level == "silent":
        return log_id
    if level in ("error", "fatal_error"):
        from utils.notify import notify_failure
        notify_failure(f"log#{log_id}: {message}")
    return log_id
