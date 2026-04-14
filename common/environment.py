import threading
from contextvars import ContextVar
from typing import NamedTuple

from db import env_get, db_get

_resume_event = threading.Event()

asserted_log_entry: ContextVar[bool] = ContextVar('asserted_log_entry', default=False)

_resume_event.set()

_wakeup_event = threading.Event()

_threads_lock    = threading.Lock()
_active_threads  = 0
_active_batch_ids: set = set()


def set_running():
    _resume_event.set()


def set_paused():
    _resume_event.clear()


def wait_if_paused():
    _resume_event.wait()


def wakeup_loop():
    _wakeup_event.set()


def wait_for_wakeup(timeout: int):
    _wakeup_event.wait(timeout=timeout)
    _wakeup_event.clear()


def get_active_threads() -> int:
    with _threads_lock:
        return _active_threads


def reset_active_threads():
    global _active_threads
    with _threads_lock:
        _active_threads = 0
        _active_batch_ids.clear()


def claim_batch(batch_id: str) -> bool:
    """Добавляет batch_id в активные. Возвращает False если уже занят."""
    global _active_threads
    with _threads_lock:
        if batch_id in _active_batch_ids:
            return False
        _active_batch_ids.add(batch_id)
        _active_threads += 1
        return True


def release_batch(batch_id: str):
    """Освобождает batch_id из активных."""
    global _active_threads
    with _threads_lock:
        _active_batch_ids.discard(batch_id)
        if _active_threads > 0:
            _active_threads -= 1


def is_batch_active(batch_id: str) -> bool:
    with _threads_lock:
        return batch_id in _active_batch_ids


def get_active_batch_ids() -> set:
    with _threads_lock:
        return set(_active_batch_ids)


class EnvSnapshot(NamedTuple):
    deep_logging:   bool
    emulation_mode: bool
    use_donor:      bool
    loop_interval:  int
    max_threads:    int


_current: EnvSnapshot = EnvSnapshot(
    deep_logging=False,
    emulation_mode=False,
    use_donor=True,
    loop_interval=15,
    max_threads=5,
)

deep_logging:   bool = _current.deep_logging
emulation_mode: bool = _current.emulation_mode
use_donor:      bool = _current.use_donor
loop_interval:  int  = _current.loop_interval
max_threads:    int  = _current.max_threads


def snapshot() -> EnvSnapshot:
    """Возвращает неизменяемый снимок текущего окружения.
    Потоки вызывают один раз в начале работы и используют локально."""
    return _current


def refresh_environment() -> EnvSnapshot:
    """Читает актуальные параметры окружения из БД и обновляет снимок."""
    global deep_logging, emulation_mode, use_donor, loop_interval, max_threads, _current
    snap = EnvSnapshot(
        deep_logging   = db_get('deep_debugging',    '0') == '1',
        emulation_mode = env_get('emulation_mode',   '0') == '1',
        use_donor      = env_get('use_donor',        '1') == '1',
        loop_interval  = max(1, min(3600, int(db_get('loop_interval',     '15')))),
        max_threads    = max(1, min(32,   int(db_get('max_batch_threads', '5')))),
    )
    _current       = snap
    deep_logging   = snap.deep_logging
    emulation_mode = snap.emulation_mode
    use_donor      = snap.use_donor
    loop_interval  = snap.loop_interval
    max_threads    = snap.max_threads
    return snap


def init_from_db():
    """Инициализирует состояние workflow из БД."""
    reset_active_threads()
    if env_get('workflow_state', 'running') == 'pause':
        set_paused()
    else:
        set_running()
