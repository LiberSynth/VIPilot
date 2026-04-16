from datetime import timedelta

from flask import Flask

from db import (
    db_reset_stalled_batches,
    db_get_distinct_batch_statuses,
)
from db.connection import get_db
from common.statuses import FINAL_BATCH_STATUSES, _assert_known_status
from log.log import write_log, write_log_entry
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
    from routes.api import bp as api_bp, producer_bp, operator_bp
    from routes.dzen_browser import bp as dzen_browser_bp

    app.register_blueprint(web_bp)
    app.register_blueprint(api_bp)
    app.register_blueprint(producer_bp)
    app.register_blueprint(operator_bp)
    app.register_blueprint(dzen_browser_bp)

    _reset_stalled_batches()
    _interrupt_running_pipelines()
    _fix_orphaned_running()
    _validate_batch_statuses()


def _reset_stalled_batches():
    affected = db_reset_stalled_batches()
    if not affected:
        write_log_entry(None, "[startup] Незавершённых батчей не обнаружено.", level='silent')
        return
    for item in affected:
        bid = item["id"]
        old = item["old_status"]
        new = item["new_status"]
        msg = f"Батч сброшен при рестарте: {old} → {new}"
        write_log('planning', msg, status='warn', batch_id=bid)
        write_log_entry(None, fmt_id_msg("[startup] Батч {} сброшен: {} → {}", bid, old, new), level='silent')


def _interrupt_running_pipelines():
    for pipeline in ('story', 'video', 'transcode', 'publish'):
        try:
            with get_db() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE log SET status = 'cancelled' WHERE pipeline = %s AND status = 'running'",
                        (pipeline,),
                    )
                    count = cur.rowcount
                conn.commit()
            if count:
                write_log_entry(None, f"[startup] {pipeline}: {count} незавершённых записей → cancelled", level='silent')
        except Exception as e:
            write_log_entry(None, f"[DB] Ошибка прерывания running для {pipeline}: {e}", level='silent')


def _fix_orphaned_running():
    try:
        placeholders = ', '.join(['%s'] * len(FINAL_BATCH_STATUSES))
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT l.id, l.pipeline, l.batch_id, b.status
                    FROM log l
                    JOIN batches b ON b.id = l.batch_id
                    WHERE l.status = 'running'
                      AND b.status IN ({placeholders})
                    """,
                    FINAL_BATCH_STATUSES,
                )
                rows = cur.fetchall()
                if rows:
                    for row in rows:
                        log_id, pipeline, batch_id, batch_status = row
                        write_log_entry(
                            log_id,
                            fmt_id_msg(
                                "[startup] WARN: лог #{} (pipeline={}, batch={}) завис в 'running', "
                                "батч уже в статусе '{}'",
                                log_id, pipeline, batch_id, batch_status
                            ),
                            level='silent',
                        )
                    ids = [r[0] for r in rows]
                    cur.execute(
                        "UPDATE log SET status = 'cancelled' WHERE id = ANY(%s)",
                        (ids,),
                    )
                    conn.commit()
                    write_log_entry(None, f"[startup] {len(ids)} зависших 'running' записей → cancelled", level='silent')
    except Exception as e:
        write_log_entry(None, f"[DB] Ошибка _fix_orphaned_running: {e}", level='silent')


def _validate_batch_statuses():
    for status in db_get_distinct_batch_statuses():
        _assert_known_status(status)
    write_log_entry(None, "[startup] Все статусы батчей известны.", level='silent')
