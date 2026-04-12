from datetime import timedelta

from flask import Flask

from db import (
    db_reset_stalled_batches,
    db_get_batches_with_unknown_status,
)
from statuses import KNOWN_BATCH_STATUSES, COMPOSITE_BATCH_STATUS_SUFFIXES
from log.log import db_log_pipeline
from utils.consts import FLASK_SECRET
from utils.limiter import limiter


def create_app() -> Flask:
    app = Flask('main', static_folder="static", static_url_path="/static")
    app.secret_key = FLASK_SECRET
    app.permanent_session_lifetime = timedelta(days=7)
    limiter.init_app(app)
    return app


def init_app(app: Flask):
    from routes.web import bp as web_bp
    from routes.api import bp as api_bp, producer_bp
    from routes.dzen_browser import bp as dzen_browser_bp

    app.register_blueprint(web_bp)
    app.register_blueprint(api_bp)
    app.register_blueprint(producer_bp)
    app.register_blueprint(dzen_browser_bp)

    _reset_stalled_batches()
    _validate_batch_statuses()


def _reset_stalled_batches():
    affected = db_reset_stalled_batches()
    if not affected:
        print("[startup] Незавершённых батчей не обнаружено.")
        return
    for item in affected:
        bid = item["id"]
        old = item["old_status"]
        new = item["new_status"]
        msg = f"Батч сброшен при рестарте: {old} → {new}"
        db_log_pipeline('startup', msg, status='warn', batch_id=bid)
        print(f"[startup] Батч {bid[:8]}… сброшен: {old} → {new}")


def _validate_batch_statuses():
    unknown_batches = db_get_batches_with_unknown_status(KNOWN_BATCH_STATUSES)
    truly_unknown = {
        batch_id: status
        for batch_id, status in unknown_batches.items()
        if not any(status.endswith(sfx) for sfx in COMPOSITE_BATCH_STATUS_SUFFIXES)
    }
    if truly_unknown:
        for batch_id, status in truly_unknown.items():
            msg = f"[validate] ВНИМАНИЕ: батч имеет неизвестный статус: {status!r}"
            db_log_pipeline('validate', msg, status='error', batch_id=batch_id)
            print(f"[validate] batch_id={batch_id}: неизвестный статус {status!r}")
    else:
        print("[validate] Все статусы батчей известны.")
