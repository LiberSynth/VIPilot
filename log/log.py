"""
Все функции логирования приложения.
"""

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


def log_app_started():
    global _lifecycle_stop_logged, _system_log_id
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

    batch_id=None — системное окно между батчами (system_log_id).
    batch_id задан — сбрасывает system_log_id и пишет в log (batch_id, category).
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

    print(message)
    if level == "silent" and not environment.deep_debugging:
        return None

    if batch_id is not None:
        _system_log_id = None
        mod_token = environment.asserted_log_modification.set(True)
        entry_token = environment.asserted_log_entry.set(True)
        try:
            log_id = db_get_or_create_log(batch_id, category)
            db_insert_log_entry(log_id, message, level)
        finally:
            environment.asserted_log_entry.reset(entry_token)
            environment.asserted_log_modification.reset(mod_token)
    else:
        mod_token = environment.asserted_log_modification.set(True)
        entry_token = environment.asserted_log_entry.set(True)
        try:
            if _system_log_id is None:
                _system_log_id = db_insert_log(None, category)
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
