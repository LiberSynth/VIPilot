"""
Общие утилиты пайплайнов.

Содержит повторяющиеся блоки, которые иначе копировались бы
в каждый пайплайн дословно.
"""

from db import (
    db_set_batch_fatal_error,
    db_set_batch_cancelled,
    db_is_batch_scheduled,
)
from log import db_log_pipeline, db_log_entry, db_log_update
from utils.notify import notify_failure


def check_cancelled(pipeline_name: str, batch_id: str, batch: dict) -> bool:
    """Проверяет, не отменён ли батч (слот расписания удалён или таргет отключён).

    Возвращает True, если батч отменён и пайплайн должен прерваться.
    Вызывать только для не-пробных батчей (is_probe проверяет сам вызывающий).
    """
    if not db_is_batch_scheduled(batch['scheduled_at'], batch['target_id']):
        db_set_batch_cancelled(batch_id)
        db_log_pipeline(
            pipeline_name,
            'Батч отменён — слот удалён из расписания или таргет отключён',
            status='прервана',
            batch_id=batch_id,
        )
        print(f"[{pipeline_name}] Батч {batch_id[:8]}… отменён, пропускаю")
        return True
    return False


def handle_critical_error(pipeline_name: str, batch_id: str, log_id, e: Exception) -> None:
    """Обрабатывает непойманное исключение в пайплайне:
    переводит батч в fatal_error, записывает в лог и отправляет уведомление.
    """
    msg = f"Критическая ошибка приложения: {e}"
    db_set_batch_fatal_error(batch_id)
    if log_id:
        db_log_update(log_id, msg, 'error')
        db_log_entry(log_id, msg, level='error')
    else:
        new_log_id = db_log_pipeline(pipeline_name, msg, status='error', batch_id=batch_id)
        if new_log_id:
            db_log_entry(new_log_id, msg, level='error')
    print(f"[{pipeline_name}] Ошибка: {e}")
    notify_failure(f"{pipeline_name}: критическая ошибка — {e}")
