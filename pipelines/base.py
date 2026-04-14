"""
Общие утилиты пайплайнов.

Содержит повторяющиеся блоки, которые иначе копировались бы
в каждый пайплайн дословно.

Правило логирования: внутри файлов pipelines/ разрешён только write_log_entry.
Прямые вызовы print запрещены — guard-обёртка _forbidden_print автоматически
подставляется в пространство имён каждого модуля пакета через pipelines/__init__.py.
"""

from db import (
    db_set_batch_status,
    db_is_batch_scheduled,
)
from log import write_log_entry, db_log_update
from utils.utils import fmt_id_msg


def _forbidden_print(*args, **kwargs):
    """Guard: прямой вызов print в файлах pipelines/ запрещён.
    Используйте write_log_entry вместо этого.
    """
    raise AssertionError(
        "Прямой вызов print в pipelines/ запрещён. "
        "Используйте write_log_entry(log_id, msg, level) из log."
    )


def check_cancelled(pipeline_name: str, batch_id: str, batch: dict, log_id=None) -> bool:
    """Проверяет, не отменён ли батч (слот расписания удалён).

    Возвращает True, если батч отменён и пайплайн должен прерваться.
    Вызывать только для не-пробных батчей (is_probe проверяет сам вызывающий).
    log_id — идентификатор текущей записи лога (для обновления статуса).
    """
    if not db_is_batch_scheduled(batch['scheduled_at'], batch.get('type', 'slot')):
        db_set_batch_status(batch_id, 'cancelled')
        msg = 'Батч отменён — слот удалён из расписания'
        if log_id:
            db_log_update(log_id, msg, 'cancelled')
        write_log_entry(log_id, fmt_id_msg("[{}] Батч {} отменён, пропускаю", pipeline_name, batch_id))
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
