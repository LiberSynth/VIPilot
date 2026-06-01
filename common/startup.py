from datetime import timedelta

from flask import Flask

from db import (
    db_reset_stalled_batches,
)
from log.log import write_log_entry
from utils.consts import FLASK_SECRET
from utils.limiter import limiter
from utils.utils import fmt_id_msg


def create_app() -> Flask:
    app = Flask('main', static_folder="static", static_url_path="/static")
    app.secret_key = FLASK_SECRET
    app.permanent_session_lifetime = timedelta(days=7)
    limiter.init_app(app)
    return app


def init_app(app: Flask):
    from routes.web import bp as web_bp
    from routes.api import bp as api_bp, production_bp
    from routes.browser_widget import bp as browser_widget_bp

    app.register_blueprint(web_bp)
    app.register_blueprint(api_bp)
    app.register_blueprint(production_bp)
    app.register_blueprint(browser_widget_bp)

    _reset_stalled_batches()


def _reset_stalled_batches():
    affected = db_reset_stalled_batches()
    if not affected:
        write_log_entry(None, "system", "[startup] Незавершённых батчей не обнаружено.", level='silent')
        return
    for item in affected:
        bid = item["id"]
        old = item["old_status"]
        new = item["new_status"]
        msg = f"Батч сброшен при рестарте: {old} → {new}"
        write_log_entry(bid, "planning", msg, level='warn')
        write_log_entry(None, "system", fmt_id_msg("[startup] Батч {} сброшен: {} → {}", bid, old, new), level='silent')
