"""Подхват батчей, прерванных при остановке приложения."""
from db import db_reset_stalled_batches
from log.log import write_log_entry
from utils.utils import fmt_id_msg

def recover_interrupted_batches() -> None:
    affected = db_reset_stalled_batches()
    if not affected:
        write_log_entry(None, 'startup', 'Незавершённых батчей не обнаружено.', level='silent')
        return
    for item in affected:
        bid = item["id"]
        old = item["old_status"]
        new = item["new_status"]
        msg = f"Батч сброшен при рестарте: {old} → {new}"
        write_log_entry(bid, "planning", msg, level='warn')
        write_log_entry(bid, 'planning', fmt_id_msg('Батч {} сброшен: {} → {}', bid, old, new), level='silent')
