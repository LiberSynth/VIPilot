"""
Pipeline 3 — Генерация видео.

stage-batch type=movie:
  pending    -> submit запроса провайдеру
  processing -> polling статуса у провайдера
  processed  -> загрузка/сохранение видео
  ready      -> финал
"""

import common.environment as environment
from utils.prompt_params import apply_prompt_params
from db import (
    settings_get,
    cycle_config_get,
    db_get_batch_by_id,
    db_get_story_prompt,
    db_get_story_title,
    db_get_video_model_by_id,
    db_save_video_job_and_set_pending,
    db_save_video_job_and_set_processing,
    db_save_generated_video_url,
    db_get_batch_original_video,
    db_set_batch_status,
    db_create_batch_movie,
)
from log import write_log_entry
from common.exceptions import AppException
from clients import falai, grok, skyreels
from routes.api import client_is_configured
from clients.falai import ProviderFatalError
from utils.utils import fmt_id_msg, nearest_allowed_duration

def _video_client(platform_name: str):
    """Возвращает клиент нужной платформы по имени."""
    if platform_name == 'Grok':
        return grok
    if platform_name == 'SkyReels':
        return skyreels
    return falai

def _is_content_moderation_error(err_text: str) -> bool:
    low = (err_text or '').lower()
    return 'moderation' in low or 'политик' in low or 'content policy' in low

def _clear_handshake_payload() -> dict:
    return {
        'request_id': None,
        'status_url': None,
        'response_url': None,
        'generated_video_url': None,
    }

def _movie_pending_resume_target(batch_data) -> str | None:
    """Если pending, но в data есть прогресс — куда подхватить без нового submit."""
    if not isinstance(batch_data, dict):
        return None
    if batch_data.get('generated_video_url'):
        return 'processed'
    if (
        batch_data.get('request_id')
        and batch_data.get('status_url')
        and batch_data.get('response_url')
    ):
        return 'processing'
    return None

def _download_and_finalize(batch_id, batch, category, batch_data, used_model, used_model_id):
    # Подхват после падения между сохранением файла и выставлением ready:
    # если movie_id уже есть и raw-файл доступен, просто доводим статус.
    if batch.get('movie_id'):
        existing_raw = db_get_batch_original_video(batch_id)
        if existing_raw is not None:
            db_set_batch_status(batch_id, 'ready')
            msg = f'Видео сгенерировано ({used_model})' if used_model else 'Видео сгенерировано'
            write_log_entry(batch_id, category, 'Готово: batch → ready (файл уже сохранён)')
            write_log_entry(
                batch_id, category,
                fmt_id_msg("[video] Батч {} — phase=run_done, status=ready, model_id={}", batch_id, used_model_id),
                level='silent',
            )
            return

    video_url = batch_data.get('generated_video_url') if isinstance(batch_data, dict) else None
    if not video_url:
        msg = 'Для статуса processed отсутствует generated_video_url — невозможна загрузка видео'
        write_log_entry(batch_id, category, msg, level='error')
        raise AppException(batch_id, 'video', msg)

    write_log_entry(batch_id, category, 'Скачиваю оригинал от провайдера.')
    write_log_entry(batch_id, category, f"Скачиваю оригинал: {video_url}", level='silent')
    try:
        original_data = falai.download_video(batch_id, category, video_url)
        orig_mb = round(len(original_data) / 1024 / 1024, 1)
        db_create_batch_movie(batch_id, original_data, video_url, used_model_id)
        write_log_entry(batch_id, category, "Оригинал сохранён в базе данных.")
        write_log_entry(batch_id, category, f"Оригинал сохранён в БД ({orig_mb} МБ)", level='silent')
    except Exception as e:
        msg = f"Ошибка скачивания оригинала: {e}"
        write_log_entry(batch_id, category, msg, level='error')
        write_log_entry(batch_id, category, f"{msg}", level='silent')
        raise AppException(batch_id, 'video', msg)

    db_set_batch_status(batch_id, 'ready')
    msg = f'Видео сгенерировано ({used_model})' if used_model else 'Видео сгенерировано'
    write_log_entry(batch_id, category, 'Готово: batch → ready')
    write_log_entry(batch_id, category, f"URL: {video_url}", level='silent')
    write_log_entry(
        batch_id, category,
        fmt_id_msg("[video] Батч {} — phase=run_done, status=ready, model_id={}", batch_id, used_model_id),
        level='silent',
    )

def run(batch_id, category):
    _snap = environment.snapshot()
    try:
        batch = db_get_batch_by_id(batch_id)
        if not batch:
            return

        status = batch['status']
        write_log_entry(
            batch_id, category,
            fmt_id_msg("[video] Батч {} — phase=run_start, status={}", batch_id, status),
            level='silent',
        )

        if batch.get('type') != 'movie':
            msg = f"Неподдерживаемый тип батча для video-пайплайна: {batch.get('type')}"
            write_log_entry(batch_id, category, msg, level='error')
            raise AppException(batch_id, 'video', msg)

        if status not in ('pending', 'processing', 'processed'):
            return

        batch_data = batch.get('data') or {}
        if status == 'pending':
            resume_target = _movie_pending_resume_target(batch_data)
            if resume_target:
                if resume_target == 'processed':
                    write_log_entry(batch_id, category, 'Подхват: готовый URL — загрузка без нового запроса.')
                else:
                    write_log_entry(batch_id, category, 'Подхват: возобновление опроса предыдущего запроса.')
                db_set_batch_status(batch_id, resume_target)
                status = resume_target
                batch = db_get_batch_by_id(batch_id)
                batch_data = (batch.get('data') or {}) if batch else batch_data
        model_id = batch_data.get('model_id') if isinstance(batch_data, dict) else None
        if not model_id:
            msg = "Для movie-батча не задан data.model_id"
            write_log_entry(batch_id, category, msg, level='error')
            raise AppException(batch_id, 'video', msg)

        model = db_get_video_model_by_id(model_id)
        if not model:
            msg = fmt_id_msg("Модель {} не найдена", model_id)
            write_log_entry(batch_id, category, msg, level='error')
            raise AppException(batch_id, 'video', msg)

        model_name = model['name']
        model_platform = model.get('platform_name', '')
        client = _video_client(model_platform)

        # movie stage всегда запускается как ручная генерация из режиссёра.
        ar_x = 9
        ar_y = 16
        write_log_entry(
            batch_id, category,
            fmt_id_msg(
                "[video] Батч {} — phase=target_resolved, target=ручной, ar={}:{}",
                batch_id, ar_x, ar_y,
            ),
            level='silent',
        )

        story_id = batch.get('story_id')
        if story_id is None:
            raise RuntimeError('story_id отсутствует у батча movie — ошибка логики')
        story_id = str(story_id)

        if status == 'processed':
            _download_and_finalize(batch_id, batch, category, batch_data, model_name, model_id)
            return

        if not client_is_configured('falai'):
            msg = 'FAL_API_KEY не задан — генерация невозможна'
            write_log_entry(batch_id, category, msg, level='error')
            write_log_entry(batch_id, category, f"{msg}", level='silent')
            raise AppException(batch_id, 'video', msg)

        # processing: строго polling уже существующего запроса (без submit).
        if status == 'processing':
            request_id = batch_data.get('request_id') if isinstance(batch_data, dict) else None
            status_url = batch_data.get('status_url') if isinstance(batch_data, dict) else None
            response_url = batch_data.get('response_url') if isinstance(batch_data, dict) else None

            if not request_id or not status_url or not response_url:
                write_log_entry(batch_id, category, 'Для статуса processing отсутствует handshake, сбрасываю в pending.', level='warn')
                db_save_video_job_and_set_pending(batch_id, _clear_handshake_payload())
                return
            write_log_entry(
                batch_id, category,
                f"Возобновление генерации видео, сюжет {db_get_story_title(story_id) or '(без названия)'}, модель {model_name}.",
            )
            write_log_entry(batch_id, category, "Возобновление предыдущего запроса.")
            write_log_entry(batch_id, category, fmt_id_msg("Возобновление: request_id={}", request_id), level='silent')

            video_url, poll_err = client.poll(batch_id, category, status_url, response_url)
            if video_url:
                db_save_generated_video_url(batch_id, video_url)
                write_log_entry(batch_id, category, "Провайдер сообщил: видео готово (batch → processed).")
                write_log_entry(
                    batch_id, category,
                    fmt_id_msg("[video] Батч {} — phase=poll_resume_success, request_id={}", batch_id, request_id),
                    level='silent',
                )
                batch = db_get_batch_by_id(batch_id)
                _download_and_finalize(
                    batch_id,
                    batch,
                    category,
                    batch.get('data') or {},
                    model_name,
                    model_id,
                )
                return

            if poll_err:
                write_log_entry(batch_id, category, f"Поллинг завершился с ошибкой: {poll_err}", level='warn')
                write_log_entry(
                    batch_id, category,
                    fmt_id_msg(
                        "[video] Батч {} — phase=poll_resume_error, request_id={}, error={}",
                        batch_id, request_id, poll_err,
                    ),
                    level='silent',
                )
                if _is_content_moderation_error(poll_err):
                    msg = 'Видео отклонено модерацией контента провайдера'
                    write_log_entry(batch_id, category, msg, level='error')
                    write_log_entry(batch_id, category, f"{msg}", level='silent')
                    raise AppException(batch_id, 'video', msg)
                # Немодерационная ошибка: новый запуск с pending.
                db_save_video_job_and_set_pending(batch_id, _clear_handshake_payload())
                return

            write_log_entry(batch_id, category, "Провайдер ещё не завершил генерацию, оставляю статус processing.", level='silent')
            return

        # pending: новый submit одной конкретной модели.
        try:
            max_attempts_per_model = int(settings_get('video_fails_to_next', '3'))
        except (ValueError, TypeError):
            max_attempts_per_model = 3

        video_prompt = db_get_story_prompt(story_id)
        if not video_prompt:
            msg = fmt_id_msg('T2V-промпт не задан для story_id={}', story_id)
            write_log_entry(batch_id, category, msg, level='error')
            write_log_entry(batch_id, category, f"{msg}", level='silent')
            raise AppException(batch_id, 'video', msg)

        story_title = db_get_story_title(story_id) or '(без названия)'
        video_duration = int(cycle_config_get('video_duration'))
        allowed = model.get('allowed_durations') or [0]
        actual_duration = nearest_allowed_duration(video_duration, allowed)
        if actual_duration != video_duration:
            write_log_entry(
                batch_id, category,
                f"Длительность скорректирована: {video_duration}с → {actual_duration}с (модель поддерживает: {allowed})",
                level='warn',
            )
        video_post_prompt = cycle_config_get('video_post_prompt').strip()
        if video_post_prompt:
            video_post_prompt = apply_prompt_params(
                video_post_prompt,
                duration_seconds=actual_duration,
            )
            video_prompt = video_prompt + '\n\n' + video_post_prompt

        write_log_entry(batch_id, category, f"Начало генерации видео, сюжет {story_title}, модель {model_name}.")
        write_log_entry(batch_id, category, f"Соотношение сторон: {ar_x}:{ar_y}", level='silent')
        write_log_entry(batch_id, category, f"Ручная модель: {model_name}, попыток: {max_attempts_per_model}", level='silent')

        for attempt in range(1, max_attempts_per_model + 1):
            if attempt > 1:
                write_log_entry(batch_id, category, f"[{model_name}] попытка {attempt}/{max_attempts_per_model}", level='warn')
            else:
                write_log_entry(batch_id, category, "Отправляю запрос к провайдеру.")
                write_log_entry(batch_id, category, f"Запрос к провайдеру: модель={model_name}, ar={ar_x}:{ar_y}", level='silent')

            sr = client.submit(
                batch_id, category, model_name, model['submit_url'], model['platform_url'],
                model['body_tpl'], video_prompt, ar_x, ar_y, actual_duration,
            )
            if not sr:
                write_log_entry(batch_id, category, "Провайдер не принял запрос — переход к следующей попытке.", level='warn')
                continue

            req_id = sr['request_id']
            s_url = sr['status_url']
            r_url = sr['response_url']

            db_save_video_job_and_set_processing(
                batch_id,
                {
                    'request_id': req_id,
                    'status_url': s_url,
                    'response_url': r_url,
                    'generated_video_url': None,
                },
            )
            write_log_entry(batch_id, category, "Запрос принят, ожидаю результат.")
            write_log_entry(batch_id, category, fmt_id_msg("[video] Генерация запущена: request_id={}", req_id), level='silent')

            video_url, poll_err = client.poll(batch_id, category, s_url, r_url)
            if video_url:
                db_save_generated_video_url(batch_id, video_url)
                write_log_entry(batch_id, category, "Провайдер сообщил: видео готово (batch → processed).")
                batch = db_get_batch_by_id(batch_id)
                _download_and_finalize(
                    batch_id,
                    batch,
                    category,
                    batch.get('data') or {},
                    model_name,
                    model_id,
                )
                return

            if poll_err:
                write_log_entry(batch_id, category, f"Поллинг завершился с ошибкой: {poll_err}", level='warn')
                if _is_content_moderation_error(poll_err):
                    msg = 'Видео отклонено модерацией контента провайдера'
                    write_log_entry(batch_id, category, msg, level='error')
                    write_log_entry(batch_id, category, f"{msg}", level='silent')
                    raise AppException(batch_id, 'video', msg)
                db_save_video_job_and_set_pending(batch_id, _clear_handshake_payload())
                continue

            # Запрос активен, продолжаем ждать в processing.
            write_log_entry(batch_id, category, 'Провайдер ещё не завершил генерацию, продолжаю ожидание.', level='silent')
            return

        msg = f'Модель {model_name} не дала результата ({max_attempts_per_model} попыток)'
        write_log_entry(batch_id, category, msg, level='error')
        write_log_entry(batch_id, category, f"{msg}", level='silent')
        raise AppException(batch_id, 'video', msg)

    except ProviderFatalError as e:
        msg = f"Фатальная ошибка провайдера: {e}"
        write_log_entry(batch_id, category, msg, level='error')
        write_log_entry(batch_id, category, f"{msg}", level='silent')
        raise AppException(batch_id, 'video', msg)
