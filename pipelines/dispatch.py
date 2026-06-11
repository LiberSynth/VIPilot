"""
Резервирование батча в БД перед запуском потока пайплайна.

In-memory claim_batch() защищает от параллельного dispatch в одном процессе;
CAS здесь — от повторной выдачи того же batch_id после сбоя in-memory lock.
"""

from db import db_claim_batch_status

# (type, status на входе) -> (from_status, to_status) для db_claim_batch_status
_CLAIM_AT_DISPATCH: dict[tuple[str, str], tuple[str, str]] = {
    ("story", "pending"): ("pending", "generating"),
    ("transcode", "pending"): ("pending", "processing"),
}

def prepare_batch_dispatch(batch_id: str, batch_type: str, status: str) -> bool:
    """Атомарно переводит батч в рабочий статус. False — батч уже занят или статус устарел."""
    claim = _CLAIM_AT_DISPATCH.get((batch_type, status))
    if claim is None:
        return True
    from_status, to_status = claim
    return db_claim_batch_status(batch_id, from_status, to_status)
