"""
Запуск батча в потоке с централизованной обработкой ошибок.
"""
import threading

from db import db_set_batch_status
from log import db_log_pipeline, log_entry
from exceptions import AppException
import utils.workflow_state as wf_state


def _handle_batch_error(e, batch_id, pipeline_name, log_id):
    if isinstance(e, AppException):
        db_set_batch_status(batch_id, 'error')
        log_entry(log_id, f"Ошибка пайплайна {e.pipeline}: {e.message}", level='error')
    else:
        db_set_batch_status(batch_id, 'fatal_error')
        log_entry(log_id, f"Критическая ошибка: {e}", level='fatal_error')


def run_batch(batch_id, pipeline, log_id):
    """Запускает пайплайн, обрабатывает ошибки и освобождает слот потока."""
    pipeline_name = getattr(pipeline, '__name__', str(pipeline)).split('.')[-1]
    try:
        pipeline.run(batch_id, log_id)
    except Exception as e:
        _handle_batch_error(e, batch_id, pipeline_name, log_id)
    finally:
        wf_state.release_batch(batch_id)
        wf_state.wakeup_loop()


def start_batch_thread(batch_id, pipeline, pipeline_name):
    """Создаёт log_id, запускает поток для обработки батча."""
    log_id = db_log_pipeline(pipeline_name, f'Запуск пайплайна {pipeline_name}…', status='running', batch_id=batch_id)
    t = threading.Thread(target=run_batch, args=(batch_id, pipeline, log_id), daemon=True)
    t.start()
