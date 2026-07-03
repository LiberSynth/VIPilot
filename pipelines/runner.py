"""
Запуск батча в потоке с централизованной обработкой ошибок.
"""
import threading

from db import db_get_batch_status, db_set_batch_status
from log import write_log_entry
from common.exceptions import AppException, ShutdownRequested
from common.shutdown import (
    is_playwright_shutdown_error,
    is_shutting_down,
    register_batch_thread,
    unregister_batch_thread,
)
import common.environment as environment
from utils.utils import fmt_id_msg


def _interrupt_batch_on_shutdown(batch_id: str) -> None:
    status = db_get_batch_status(batch_id)
    if not status:
        return
    if status.endswith(".posting"):
        db_set_batch_status(batch_id, status[: -len(".posting")] + ".pending")
    elif status == "processing":
        db_set_batch_status(batch_id, "pending")


def _handle_batch_error(e, batch_id, category):
    pipeline_name = category
    if isinstance(e, AppException):
        db_set_batch_status(batch_id, 'error')
        msg = f"Ошибка пайплайна {e.pipeline}: {e.message}"
        write_log_entry(batch_id, category, msg, level='error')
        write_log_entry(
            batch_id,
            'runner',
            fmt_id_msg(
                f"batch={{}}, pipeline={pipeline_name}, phase=error_handled, type=AppException",
                batch_id,
            ),
            level='silent',
        )
    else:
        db_set_batch_status(batch_id, 'fatal_error')
        msg = f"Критическая ошибка: {e}"
        write_log_entry(batch_id, category, msg, level='fatal_error')
        write_log_entry(
            batch_id,
            'runner',
            fmt_id_msg(
                f"batch={{}}, pipeline={pipeline_name}, phase=error_handled, type={type(e).__name__}",
                batch_id,
            ),
            level='silent',
        )

def run_batch(batch_id, pipeline, category):
    """Запускает пайплайн, обрабатывает ошибки и освобождает слот потока."""
    register_batch_thread(batch_id, threading.current_thread())
    write_log_entry(batch_id, category, f'Запуск пайплайна {category}.', level='info')
    write_log_entry(
        batch_id,
        'runner',
        fmt_id_msg(f"batch={{}}, pipeline={category}, phase=run_start", batch_id),
        level='silent',
    )
    try:
        pipeline.run(batch_id, category)
        write_log_entry(
            batch_id,
            'runner',
            fmt_id_msg(f"batch={{}}, pipeline={category}, phase=run_done", batch_id),
            level='silent',
        )
    except ShutdownRequested:
        _interrupt_batch_on_shutdown(batch_id)
        write_log_entry(
            batch_id, category,
            "Прервано: остановка приложения.",
            level="info",
        )
    except Exception as e:
        if is_shutting_down() and is_playwright_shutdown_error(e):
            _interrupt_batch_on_shutdown(batch_id)
            write_log_entry(
                batch_id, category,
                "Прервано: остановка приложения.",
                level="info",
            )
        else:
            _handle_batch_error(e, batch_id, category)
    finally:
        unregister_batch_thread(batch_id)
        environment.release_batch(batch_id)
        from services.browser_registry import clear_publish_frames_for_batch
        clear_publish_frames_for_batch(batch_id)
        environment.wakeup_loop()
        write_log_entry(
            batch_id,
            'runner',
            fmt_id_msg(f"batch={{}}, pipeline={category}, phase=released", batch_id),
            level='silent',
        )

def start_batch_thread(batch_id, pipeline, pipeline_name):
    """Запускает поток для обработки батча."""
    write_log_entry(
        batch_id,
        'runner',
        fmt_id_msg(f"batch={{}}, pipeline={pipeline_name}, phase=thread_start", batch_id),
        level='silent',
    )
    t = threading.Thread(target=run_batch, args=(batch_id, pipeline, pipeline_name), daemon=True)
    t.start()
