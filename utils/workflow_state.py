import threading

_resume_event = threading.Event()
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
