"""
Общие утилиты пайплайнов.

Содержит повторяющиеся блоки, которые иначе копировались бы
в каждый пайплайн дословно.

Правило логирования: внутри файлов pipelines/ разрешён только pipeline_log.
Прямые вызовы print запрещены — используйте guard-обёртку _forbidden_print,
которая автоматически подставляется в пространство имён каждого модуля пакета
через pipelines/__init__.py.
"""

import builtins as _builtins

from db import (
    db_set_batch_fatal_error,
    db_set_batch_cancelled,
    db_is_batch_scheduled,
)
from log import db_log_pipeline, log_entry, db_log_update
from utils.notify import notify_failure


def _forbidden_print(*args, **kwargs):
    """Guard: прямой вызов print в файлах pipelines/ запрещён.
    Используйте pipeline_log вместо этого.
    """
    raise AssertionError(
        "Прямой вызов print в pipelines/ запрещён. "
        "Используйте pipeline_log(log_id, msg, level) из pipelines.base."
    )


def pipeline_log(log_id, msg: str, level: str = 'info') -> None:
    """Тонкая обёртка над log_entry: пишет в БД (если log_id задан) и делает print.

    Это реэкспорт-обёртка единой функции логирования из log/.
    level: 'info' | 'warn' | 'error'
    """
    assert level in ('info', 'warn', 'error'), (
        f"pipeline_log: недопустимый уровень {level!r}. "
        "Допустимые значения: 'info', 'warn', 'error'."
    )
    log_entry(log_id, msg, level=level)
    _builtins.print(msg)


def check_cancelled(pipeline_name: str, batch_id: str, batch: dict) -> bool:
    """Проверяет, не отменён ли батч (слот расписания удалён).

    Возвращает True, если батч отменён и пайплайн должен прерваться.
    Вызывать только для не-пробных батчей (is_probe проверяет сам вызывающий).
    """
    if not db_is_batch_scheduled(batch['scheduled_at'], batch.get('type', 'slot')):
        db_set_batch_cancelled(batch_id)
        db_log_pipeline(
            pipeline_name,
            'Батч отменён — слот удалён из расписания',
            status='cancelled',
            batch_id=batch_id,
        )
        pipeline_log(None, f"[{pipeline_name}] Батч {batch_id[:8]}… отменён, пропускаю")
        return True
    return False


def handle_critical_error(pipeline_name: str, batch_id, log_id, e: Exception) -> None:
    """Обрабатывает непойманное исключение в пайплайне:
    переводит батч в fatal_error, записывает в лог и отправляет уведомление.
    """
    title = "Критическая ошибка приложения"
    msg = f"{title}: {e}"
    if batch_id:
        db_set_batch_fatal_error(batch_id)
    if log_id:
        db_log_update(log_id, title, 'error')
        log_entry(log_id, msg, level='error')
    else:
        new_log_id = db_log_pipeline(pipeline_name, msg, status='error', batch_id=batch_id)
        if new_log_id:
            log_entry(new_log_id, msg, level='error')
    _builtins.print(f"[{pipeline_name}] Ошибка: {e}")
    notify_failure(f"{pipeline_name}: критическая ошибка — {e}")
