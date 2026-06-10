import os
from datetime import timedelta

from flask import Flask

from utils.consts import FLASK_SECRET, PLAYWRIGHT_BROWSERS_PATH
from utils.limiter import limiter


def create_app() -> Flask:
    app = Flask('main', static_folder="static", static_url_path="/static")
    app.secret_key = FLASK_SECRET
    app.permanent_session_lifetime = timedelta(days=7)
    limiter.init_app(app)
    return app


def init_app(app: Flask) -> None:
    os.environ.setdefault('PLAYWRIGHT_BROWSERS_PATH', PLAYWRIGHT_BROWSERS_PATH)
