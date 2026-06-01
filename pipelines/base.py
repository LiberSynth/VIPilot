"""
Общие утилиты пайплайнов.

Содержит повторяющиеся блоки, которые иначе копировались бы
в каждый пайплайн дословно.

Правило логирования: внутри файлов pipelines/ разрешён только write_log_entry.
Прямые вызовы print запрещены — guard-обёртка _forbidden_print автоматически
подставляется в пространство имён каждого модуля пакета через pipelines/__init__.py.
"""

import os
import subprocess

from db import (
    db_set_batch_status,
    db_is_batch_scheduled,
)
from log import write_log_entry
from utils.utils import fmt_id_msg


def _forbidden_print(*args, **kwargs):
    """Guard: прямой вызов print в файлах pipelines/ запрещён.
    Используйте write_log_entry вместо этого.
    """
    raise AssertionError(
        "Прямой вызов print в pipelines/ запрещён. "
        "Используйте write_log_entry(batch_id, category, msg, level) из log."
    )


def ensure_playwright_chromium(batch_id, category) -> None:
    """Проверяет наличие Chromium для Playwright и устанавливает его, если бинарник отсутствует.

    Вызывать перед публикацией через браузерные платформы (dzen, rutube, vkvideo).
    Бросает RuntimeError если установка не удалась.
    """
    from playwright.sync_api import sync_playwright

    try:
        with sync_playwright() as p:
            exec_path = p.chromium.executable_path
    except Exception as e:
        exec_path = None
        write_log_entry(batch_id, category, f'[playwright] Не удалось определить путь к Chromium: {e}', level='silent')

    if exec_path and os.path.exists(exec_path):
        write_log_entry(batch_id, category, f'[playwright] Chromium найден: {exec_path}', level='silent')
        return

    write_log_entry(batch_id, category, 'Playwright Chromium не установлен — выполняю установку…')
    import sys
    cmd = [sys.executable, '-m', 'playwright', 'install', 'chromium', 'chromium-headless-shell']
    write_log_entry(batch_id, category, f"[playwright] install cmd={' '.join(cmd)}", level='silent')
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=300,
    )
    if result.returncode != 0:
        msg = f'playwright install chromium завершился с ошибкой: {result.stderr.strip()}'
        write_log_entry(batch_id, category, msg, level='error')
        raise RuntimeError(msg)
    write_log_entry(batch_id, category, '[playwright] install completed successfully', level='silent')
    write_log_entry(batch_id, category, 'Playwright Chromium успешно установлен')


def check_cancelled(pipeline_name: str, batch_id: str, batch: dict, category=None) -> bool:
    """Проверяет, не отменён ли батч (слот расписания удалён).

    Возвращает True, если батч отменён и пайплайн должен прерваться.
    Вызывать только для не-ручных батчей (is_manual проверяет сам вызывающий).
    category — категория лога пайплайна (по умолчанию pipeline_name).
    """
    if not db_is_batch_scheduled(batch['scheduled_at'], batch.get('type', 'planning')):
        db_set_batch_status(batch_id, 'cancelled')
        cat = category or pipeline_name
        write_log_entry(batch_id, cat, fmt_id_msg("[{}] Батч {} отменён, пропускаю", pipeline_name, batch_id), level='silent')
        return True
    return False


def iterate_models(models, max_attempts_per_model, callback, max_passes=5):
    """Перебирает модели с повторными проходами до первого успешного результата.

    Логика:
    - Внешний цикл: до max_passes проходов по всему списку моделей.
    - Средний цикл: перебор моделей.
    - Внутренний цикл: до max_attempts_per_model попыток на каждую модель.
    - На каждой итерации вызывается callback(model).
      Коллбек возвращает результат при успехе или None при неудаче.
    - При успешном возврате — выход из всех циклов, возврат результата.
    - При исчерпании всех попыток/проходов — возврат None.

    Manual-режим (одна модель, без повторных проходов) задаётся снаружи:
    передайте models с одним элементом и max_passes=1.
    """
    for _pass in range(max_passes):
        for m in models:
            for _attempt in range(max_attempts_per_model):
                result = callback(m)
                if result is not None:
                    return result
    return None
