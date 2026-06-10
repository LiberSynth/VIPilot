import atexit
import os
import signal
from datetime import timedelta

from flask import Flask

from log.log import log_app_stopped
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
        log_app_stopped()

    def _handler(signum, _frame):
        log_app_stopped()
        if signum == signal.SIGINT:
            # Preserve standard Ctrl+C behavior in dev console.
            signal.default_int_handler(signum, _frame)
        raise SystemExit(0)

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
