"""
Запуск батча в потоке с централизованной обработкой ошибок.
"""
import threading

from db import db_set_batch_status
from log import db_log_pipeline, log_entry
from utils.utils import fmt_id_msg
from exceptions import AppException
import utils.workflow_state as wf_state


def _handle_batch_error(e, batch_id, pipeline_name):
    if isinstance(e, AppException):
        db_set_batch_status(batch_id, 'error')
        msg = f"Ошибка пайплайна {e.pipeline}: {e.message}"
        lid = db_log_pipeline(e.pipeline, msg, status='error', batch_id=batch_id)
        log_entry(lid, msg, level='error')
        log_entry(None, fmt_id_msg("[{}] AppException батч {}: {}", pipeline_name, batch_id, e.message), level='silent')
    else:
        db_set_batch_status(batch_id, 'fatal_error')
        msg = f"Критическая ошибка: {e}"
        lid = db_log_pipeline(pipeline_name, msg, status='fatal_error', batch_id=batch_id)
        log_entry(lid, msg, level='fatal_error')
        log_entry(None, fmt_id_msg("[{}] Критическая ошибка батч {}: {}", pipeline_name, batch_id, e), level='silent')


def run_batch(batch_id, pipeline):
    """Запускает пайплайн, обрабатывает ошибки и освобождает слот потока."""
    pipeline_name = getattr(pipeline, '__name__', str(pipeline)).split('.')[-1]
    try:
        pipeline.run(batch_id)
    except Exception as e:
        _handle_batch_error(e, batch_id, pipeline_name)
    finally:
        wf_state.release_batch(batch_id)
        wf_state.wakeup_loop()


def start_batch_thread(batch_id, pipeline):
    """Создаёт и запускает поток для обработки батча."""
    t = threading.Thread(target=run_batch, args=(batch_id, pipeline), daemon=True)
    t.start()
