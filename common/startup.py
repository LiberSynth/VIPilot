import atexit
import os
import signal
from datetime import timedelta

from flask import Flask

import log.log as log_state
from log import write_log_entry
from common.shutdown import graceful_exit, request_shutdown
from utils.consts import FLASK_SECRET, PLAYWRIGHT_BROWSERS_PATH
from utils.limiter import limiter

def create_app() -> Flask:
    app = Flask('main', static_folder="static", static_url_path="/static")
    app.secret_key = FLASK_SECRET
    app.permanent_session_lifetime = timedelta(days=7)
    limiter.init_app(app)
    return app

def init_app() -> None:
    os.environ.setdefault('PLAYWRIGHT_BROWSERS_PATH', PLAYWRIGHT_BROWSERS_PATH)

def register_shutdown_hooks() -> None:
    def _on_exit():
        with log_state._system_log_lock:
            if log_state._lifecycle_stop_logged:
                return
            log_state._lifecycle_stop_logged = True
        write_log_entry(None, 'main', 'Приложение остановлено', level='info')

    def _handler(signum, _frame):
        request_shutdown()
        graceful_exit()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(sig, _handler)
        except (ValueError, OSError):
            pass
    if hasattr(signal, "SIGBREAK"):
        try:
            signal.signal(signal.SIGBREAK, _handler)
        except (ValueError, OSError):
            pass
    atexit.register(_on_exit)
