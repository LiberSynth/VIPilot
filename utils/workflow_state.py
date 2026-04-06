import threading

_resume_event = threading.Event()
_resume_event.set()

_wakeup_event = threading.Event()

_threads_lock = threading.Lock()
_active_threads = 0


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


def inc_active_threads():
    global _active_threads
    with _threads_lock:
        _active_threads += 1


def dec_active_threads():
    global _active_threads
    with _threads_lock:
        if _active_threads > 0:
            _active_threads -= 1


def reset_active_threads():
    global _active_threads
    with _threads_lock:
        _active_threads = 0
