"""
Все функции логирования приложения.
"""

import re
import sys
import threading

ALLOWED_LOG_CHANNELS = frozenset({
    "api",
    "planning",
    "story",
    "video",
    "transcode",
    "publish",
    "runner",
    "playwright",
    "main",
    "main_loop",
    "http",
    "db",
    "upgrade",
    "startup",
    "cleanup",
    "browser",
    "notify",
    "consts",
    "dzen",
    "rutube",
    "vkvideo",
})

_ALLOWED_LOG_CHANNELS = ALLOWED_LOG_CHANNELS

_LOG_BRACKET_PREFIX = re.compile(r"^\[([^\]]+)\]\s*", re.DOTALL)

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

_LOG_PERIOD_SKIP_TAIL = re.compile(
    r"(?:"
    r"\.{3}|…|"
    r"->|→|"
    r"https?://|"
    r"phase=|"
    r"_snap|"
    r"\b\w+=\S"
    r")",
    re.IGNORECASE,
)
_LOG_UUID_TAIL = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\s*$",
    re.IGNORECASE,
)


def _ensure_log_period(message: str) -> str:
    """Точка в конце обычных фраз; без изменений для служебных/структурных хвостов."""
    if not message:
        return message
    s = message.rstrip()
    if not s:
        return message
    last = s[-1]
    if last in ".!?…":
        return message
    if last in "=,;:})]>%»«\"'":
        return message
    if _LOG_PERIOD_SKIP_TAIL.search(s) or _LOG_UUID_TAIL.search(s):
        return message
    if last.isalnum() or last in "»«":
        return s + "."
    return message


def _resolve_channel_and_message(default_channel: str, message: str) -> tuple[str, str]:
    """Префикс [tag] в тексте → log_entries.channel, без дубля в message."""
    m = _LOG_BRACKET_PREFIX.match(message or "")
    if not m:
        return default_channel, message
    tag = m.group(1).lower()
    if tag in _ALLOWED_LOG_CHANNELS:
        return tag, message[m.end() :]
    return default_channel, message


def _stdout_log(channel: str, message: str) -> None:
    text = f"{channel} — {message}".encode("cp1251", errors="replace").decode("cp1251")
    try:
        print(text)
    except UnicodeEncodeError:
        sys.stdout.buffer.write(text.encode("cp1251", errors="replace") + b"\n")
        sys.stdout.flush()


def app_log(source: str, message: str = "", *, level: str = "silent", phase: str | None = None):
    """Лог блока «Приложение» (batch_id=None). Канал — в log_entries.channel."""
    assert source in _ALLOWED_LOG_CHANNELS, (
        f"app_log: недопустимый source {source!r}. "
        f"Допустимые: {sorted(_ALLOWED_LOG_CHANNELS)}."
    )
    body = message
    if phase:
        body = f"phase={phase}" + (f", {message}" if message else "")
    write_log_entry(None, source, body, level=level)


def log_app_started():
    global _lifecycle_stop_logged, _system_log_id
    with _system_log_lock:
        _lifecycle_stop_logged = False
        _system_log_id = None
    app_log("main", "Приложение запущено", level="info")


def log_app_stopped():
    global _lifecycle_stop_logged
    if _lifecycle_stop_logged:
        return
    _lifecycle_stop_logged = True
    app_log("main", "Приложение остановлено", level="info")


def write_log_entry(batch_id, channel, message, level="info"):
    """Единая точка записи в log и log_entries. Канал — log_entries.channel."""
    import common.environment as environment
    from db.db_simple import db_get_or_create_log, db_insert_log, db_insert_log_entry

    global _system_log_id

    channel, message = _resolve_channel_and_message(channel, message or "")

    assert channel in _ALLOWED_LOG_CHANNELS, (
        f"write_log_entry: недопустимый channel {channel!r}. "
        f"Допустимые: {sorted(_ALLOWED_LOG_CHANNELS)}."
    )
    assert level in _ALLOWED_LOG_LEVELS, (
        f"write_log_entry: недопустимый уровень {level!r}. "
        f"Допустимые: {_ALLOWED_LOG_LEVELS}."
    )

    message = _ensure_log_period(message)
    _stdout_log(channel, message)
    if level == "silent" and not environment.deep_debugging:
        return None

    mod_token = environment.asserted_log_modification.set(True)
    entry_token = environment.asserted_log_entry.set(True)
    try:
        with _system_log_lock:
            if batch_id is not None:
                log_id, is_first_batch_log = db_get_or_create_log(batch_id)
                if is_first_batch_log:
                    _system_log_id = None
                db_insert_log_entry(log_id, channel, message, level)
            else:
                if _system_log_id is None:
                    _system_log_id = db_insert_log(None)
                db_insert_log_entry(_system_log_id, channel, message, level)
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
