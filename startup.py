from db import (
    db_reset_stalled_batches,
    db_get_batches_with_unknown_status,
)
from db.db_simple import _get_dynamic_publish_statuses
from statuses import KNOWN_BATCH_STATUSES, register_dynamic_statuses
from log.log import db_log_pipeline


def init_app():
    dynamic = _get_dynamic_publish_statuses()
    register_dynamic_statuses(dynamic)
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
    from statuses import _dynamic_statuses
    all_known = KNOWN_BATCH_STATUSES | _dynamic_statuses
    unknown_batches = db_get_batches_with_unknown_status(all_known)
    if unknown_batches:
        for batch_id, status in unknown_batches.items():
            msg = f"[validate] ВНИМАНИЕ: батч имеет неизвестный статус: {status!r}"
            db_log_pipeline('validate', msg, status='error', batch_id=batch_id)
            print(f"[validate] batch_id={batch_id}: неизвестный статус {status!r}")
    else:
        print("[validate] Все статусы батчей известны.")
