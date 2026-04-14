"""
Все функции логирования приложения.
"""

_ALLOWED_PIPELINES = {
    "root",
    "planning",
    "story",
    "video",
    "transcode",
    "publish",
    "cleanup",
}

_ALLOWED_LOG_LEVELS = {
    "silent",
    "info",
    "warn",
    "error",
    "fatal_error",
}

def write_log(pipeline, message, status="info", batch_id=None):
    """Записывает событие пайплайна в таблицу log. Возвращает log_id или None."""
    assert pipeline in _ALLOWED_PIPELINES, (
        f"write_log: недопустимое имя пайплайна {pipeline!r}. "
        f"Допустимые: {_ALLOWED_PIPELINES}."
    )
    from db.db_simple import db_insert_log
    return db_insert_log(pipeline, message, status=status, batch_id=batch_id)


def write_log_entry(log_id, message, level="info"):
    """Добавляет субзапись к существующей записи лога.

    Всегда выводит сообщение в консоль.
    Если level == 'silent' — запись в БД не производится.
    Если log_id равен None — запись в log_entries вставляется с log_id=NULL.
    При уровнях 'error' и 'fatal_error' вызывает notify_failure.
    """
    import common.environment as environment
    from db.db_simple import db_insert_log_entry

    assert level in _ALLOWED_LOG_LEVELS, (
        f"write_log_entry: недопустимый уровень {level!r}. "
        f"Допустимые: {_ALLOWED_LOG_LEVELS}."
    )
    print(message)
    if level == "silent":
        return
    token = environment.asserted_log_entry.set(True)
    try:
        db_insert_log_entry(log_id, message, level)
    finally:
        environment.asserted_log_entry.reset(token)
    if level in ("error", "fatal_error"):
        from utils.notify import notify_failure
        notify_failure(f"log#{log_id}: {message}")


def log_batch_planned(batch_id, message, *entries):
    """Создаёт запись pipeline='planning' в log и добавляет субзаписи в log_entries.

    Используется при любом создании батча пользователем или планировщиком.
    batch_id — идентификатор батча.
    message  — короткий заголовок (будет записан в log.message).
    entries  — список строк, каждая добавляется как отдельная субзапись.
    Возвращает log_id или None.
    """
    log_id = write_log('planning', message, status='ok', batch_id=batch_id)
    if log_id:
        for entry_msg in entries:
            write_log_entry(log_id, entry_msg)
    return log_id
