"""
Запуск батча в потоке с централизованной обработкой ошибок.
"""
import threading

from db import db_set_batch_status
from log import write_log, db_log_update, write_log_entry
from common.exceptions import AppException
import common.environment as environment
from utils.utils import fmt_id_msg


def _runner_msg(batch_id, pipeline_name, phase, err_type=None):
    msg = f"[runner] batch={{}}, pipeline={pipeline_name}, phase={phase}"
    if err_type:
        msg += f", type={err_type}"
    return fmt_id_msg(msg, batch_id)


def _handle_batch_error(e, batch_id, pipeline_name, log_id):
    if isinstance(e, AppException):
        db_set_batch_status(batch_id, 'error')
        msg = f"Ошибка пайплайна {e.pipeline}: {e.message}"
        db_log_update(log_id, msg, 'error')
        write_log_entry(log_id, msg, level='error')
        write_log_entry(log_id, _runner_msg(batch_id, pipeline_name, "error_handled", "AppException"), level='silent')
    else:
        db_set_batch_status(batch_id, 'fatal_error')
        msg = f"Критическая ошибка: {e}"
        db_log_update(log_id, msg, 'fatal_error')
        write_log_entry(log_id, msg, level='fatal_error')
        write_log_entry(log_id, _runner_msg(batch_id, pipeline_name, "error_handled", type(e).__name__), level='silent')


def run_batch(batch_id, pipeline, log_id):
    """Запускает пайплайн, обрабатывает ошибки и освобождает слот потока."""
    pipeline_name = getattr(pipeline, '__name__', str(pipeline)).split('.')[-1]
    write_log_entry(log_id, _runner_msg(batch_id, pipeline_name, "run_start"), level='silent')
    try:
        pipeline.run(batch_id, log_id)
        write_log_entry(log_id, _runner_msg(batch_id, pipeline_name, "run_done"), level='silent')
    except Exception as e:
        _handle_batch_error(e, batch_id, pipeline_name, log_id)
    finally:
        environment.release_batch(batch_id)
        environment.wakeup_loop()
        write_log_entry(log_id, _runner_msg(batch_id, pipeline_name, "released"), level='silent')


def start_batch_thread(batch_id, pipeline, pipeline_name):
    """Создаёт log_id, запускает поток для обработки батча."""
    log_id = write_log(pipeline_name, f'Запуск пайплайна {pipeline_name}.', status='running', batch_id=batch_id)
    write_log_entry(log_id, _runner_msg(batch_id, pipeline_name, "thread_start"), level='silent')
    t = threading.Thread(target=run_batch, args=(batch_id, pipeline, log_id), daemon=True)
    t.start()
