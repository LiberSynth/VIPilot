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
    db_set_batch_status,
    db_is_batch_scheduled,
)
from log import db_log_pipeline, log_entry, db_log_update


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
    level: 'info' | 'warn' | 'error' | 'silent'
    При уровне 'silent' — только print(), без записи в БД.
    """
    assert level in ('info', 'warn', 'error', 'silent'), (
        f"pipeline_log: недопустимый уровень {level!r}. "
        "Допустимые значения: 'info', 'warn', 'error', 'silent'."
    )
    if level == 'silent':
        _builtins.print(msg)
        return
    log_entry(log_id, msg, level=level)
    _builtins.print(msg)


def check_cancelled(pipeline_name: str, batch_id: str, batch: dict) -> bool:
    """Проверяет, не отменён ли батч (слот расписания удалён).

    Возвращает True, если батч отменён и пайплайн должен прерваться.
    Вызывать только для не-пробных батчей (is_probe проверяет сам вызывающий).
    """
    if not db_is_batch_scheduled(batch['scheduled_at'], batch.get('type', 'slot')):
        db_set_batch_status(batch_id, 'cancelled')
        db_log_pipeline(
            pipeline_name,
            'Батч отменён — слот удалён из расписания',
            status='cancelled',
            batch_id=batch_id,
        )
        pipeline_log(None, f"[{pipeline_name}] Батч {batch_id[:8]}… отменён, пропускаю")
        return True
    return False


def iterate_models(models, fails_per_model, callback, max_passes=5):
    """Перебирает модели с повторными проходами до первого успешного результата.

    Логика:
    - Внешний цикл: до max_passes проходов по всему списку моделей.
    - Средний цикл: перебор моделей.
    - Внутренний цикл: до fails_per_model попыток на каждую модель.
    - На каждой итерации вызывается callback(model).
      Коллбек возвращает результат при успехе или None при неудаче.
    - При успешном возврате — выход из всех циклов, возврат результата.
    - При исчерпании всех попыток/проходов — возврат None.

    Probe-режим (одна модель, без повторных проходов) задаётся снаружи:
    передайте models с одним элементом и max_passes=1.
    """
    for _pass in range(max_passes):
        for m in models:
            for _attempt in range(fails_per_model):
                result = callback(m)
                if result is not None:
                    return result
    return None


