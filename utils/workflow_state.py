import threading

_resume_event = threading.Event()
_resume_event.set()

_wakeup_event = threading.Event()


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
