import threading

_resume_event = threading.Event()
_resume_event.set()


def set_running():
    _resume_event.set()


def set_paused():
    _resume_event.clear()


def wait_if_paused():
    _resume_event.wait()
