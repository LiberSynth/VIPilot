"""
Pipeline 3 — Генерация видео.

Принимает batch_id, определяет нужно ли возобновление (pending+request_id)
или новая генерация (pending/generating), отправляет промпт в fal.ai,
поллит статус и сохраняет video_url в batches.

Статусы батча:
  pending → generating → pending → ready
"""


import common.environment as environment
from utils.prompt_params import apply_prompt_params
from db import (
    settings_get,
    cycle_config_get,
    db_get_batch_by_id,
    db_update_batch_current_movie_model_id,
    db_claim_batch_status,
    db_get_story_text,
    db_get_story_title,
    db_get_active_video_models,
    db_get_video_model_by_id,
    db_save_video_job_and_set_pending,
    db_set_batch_status,
    db_create_batch_movie,
)
from log import db_log_update, write_log_entry
from pipelines.base import check_cancelled, iterate_models
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


def run(batch_id, log_id):
    snap = environment.snapshot()
    try:
        batch = db_get_batch_by_id(batch_id)
        if not batch:
            db_log_update(log_id, "Батч не найден", "error")
            return

        status = batch['status']
        write_log_entry(
            log_id,
            fmt_id_msg("[video] Батч {} — phase=run_start, status={}", batch_id, status),
            level='silent',
        )

        # Два допустимых входных статуса:
        # - pending:
        #   a) pending + request_id в data  -> resume поллинга
        #   b) pending + no request_id      -> CAS pending -> generating
        # - generating: прерывание до первого submit, продолжаем без CAS.
        batch_data = batch.get('data') or {}
        saved_request_id = (
            batch_data.get('request_id') if isinstance(batch_data, dict) else None
        )
        if status == 'pending':
            if saved_request_id:
                resumed = True
            else:
                resumed = False
                if not db_claim_batch_status(batch_id, 'pending', 'generating'):
                    db_log_update(log_id, "Захват батча не удался — пропуск", "cancelled")
                    return
        elif status == 'generating':
            resumed = False
        else:
            db_log_update(log_id, "Пайплайн уже выполнен — пропуск", "ok")
            return
        write_log_entry(
            log_id,
            fmt_id_msg("[video] Батч {} — phase=run_mode, resumed={}", batch_id, resumed),
            level='silent',
        )

        if batch.get('type') != 'movie':
            msg = f"Неподдерживаемый тип батча для video-пайплайна: {batch.get('type')}"
            db_log_update(log_id, msg, 'error')
            write_log_entry(log_id, msg, level='error')
            raise AppException(batch_id, 'video', msg, log_id)

        # movie stage всегда запускается как ручная генерация из режиссёра.
        is_manual = True
        target = 'ручной'
        ar_x = 9
        ar_y = 16
        write_log_entry(
            log_id,
            fmt_id_msg("[video] Батч {} — phase=target_resolved, target={}, manual={}, ar={}:{}", batch_id, target, is_manual, ar_x, ar_y),
            level='silent',
        )

        story_id        = batch.get('story_id')
        if story_id is None:
            raise RuntimeError('story_id отсутствует у батча movie в статусе pending/generating — ошибка логики')
        story_id        = str(story_id)
        pinned_model_id = batch_data.get('movie_model_id') if isinstance(batch_data, dict) else None

        video_duration = max(1, min(60, cycle_config_get('video_duration')))

        # Проверка отмены: только для slot/adhoc.
        # Manual-батч запускается пользователем явно — отменять его нет смысла.
        if not is_manual and check_cancelled('video', batch_id, batch, log_id):
            return

        if not client_is_configured('falai'):
            msg = 'FAL_API_KEY не задан — генерация невозможна'
            db_log_update(log_id, msg, 'error')
            write_log_entry(log_id, msg, level='error')
            write_log_entry(log_id, f"[video] {msg}", level='silent')
            raise AppException(batch_id, 'video', msg, log_id)

        try:
            max_attempts_per_model = max(1, int(settings_get('video_fails_to_next', '3')))
        except (ValueError, TypeError):
            max_attempts_per_model = 3

        # Выбор набора моделей:
        # - pinned_model_id (из batches.data.movie_model_id): пользователь жёстко
        #   задал модель → список из одного элемента, один проход (max_passes=1).
        # - нет pinned: перебор всех активных моделей с max_passes проходами.
        if pinned_model_id:
            pinned_m = db_get_video_model_by_id(pinned_model_id)
            if not pinned_m:
                msg = fmt_id_msg('Ручная модель {} не найдена', pinned_model_id)
                db_log_update(log_id, msg, 'error')
                write_log_entry(log_id, msg, level='error')
                write_log_entry(log_id, f"[video] {msg}", level='silent')
                raise AppException(batch_id, 'video', msg, log_id)
            models = [pinned_m]
        else:
            models = db_get_active_video_models()
            if not models:
                msg = 'Нет активных video-моделей в ai_models (type=text-to-video)'
                db_log_update(log_id, msg, 'error')
                write_log_entry(log_id, msg, level='error')
                write_log_entry(log_id, f"[video] {msg}", level='silent')
                raise AppException(batch_id, 'video', msg, log_id)

        story_text = db_get_story_text(story_id)
        if not story_text:
            msg = fmt_id_msg('Не найден сюжет story_id={}', story_id)
            db_log_update(log_id, msg, 'error')
            write_log_entry(log_id, msg, level='error')
            write_log_entry(log_id, f"[video] {msg}", level='silent')
            raise AppException(batch_id, 'video', msg, log_id)

        story_title = db_get_story_title(story_id) or '(без названия)'

        video_post_prompt = cycle_config_get('video_post_prompt').strip()
        if video_post_prompt:
            video_post_prompt = apply_prompt_params(video_post_prompt)
            story_text = story_text + '\n\n' + video_post_prompt

        db_log_update(log_id, f"Генерация видео. {'(возобновление)' if resumed else ''}".strip(), 'running')
        write_log_entry(log_id, f"Соотношение сторон: {ar_x}:{ar_y}", level='silent')
        if is_manual:
            write_log_entry(log_id, f"Ручная модель: {models[0]['name']}, попыток: {max_attempts_per_model}", level='silent')
        else:
            write_log_entry(log_id, f"Моделей: {len(models)}, попыток на модель: {max_attempts_per_model}", level='silent')

        used_model      = None
        used_model_id   = None
        attempt_counts  = {}
        first_model_log = True

        def video_callback(m):
            """Один шаг перебора: resume-поллинг или свежий submit → поллинг.

            Читает request_id из актуального batch.data:
            - Если не None — это возобновление (или уже сохранённый handshake),
              передаёт управление существующей логике поллинга.
            - Если None — обычный submit через falai.submit().

            При любом неуспехе безусловно сбрасывает все четыре поля
            (request_id, status_url, response_url, current_movie_model_id) в batch.data,
            чтобы следующая попытка шла через submit() заново.

            Возвращает video_url при успехе или None при неудаче.
            """
            nonlocal used_model, used_model_id, first_model_log
            model_name = m['name']
            cnt = attempt_counts.get(model_name, 0)
            attempt_counts[model_name] = cnt + 1
            write_log_entry(
                log_id,
                fmt_id_msg(
                    "[video] Батч {} — phase=model_callback_enter, model={}, attempt={}, resumed={}",
                    batch_id, model_name, cnt + 1, resumed,
                ),
                level='silent',
            )

            if first_model_log:
                first_model_log = False
                db_log_update(log_id, f"Генерация видео ({model_name})", 'running')
                prefix = 'Возобновление' if resumed else 'Начало'
                write_log_entry(log_id, f"{prefix} генерации видео, сюжет {story_title}, модель {model_name}.")
                write_log_entry(log_id, fmt_id_msg("[video] Батч {} ({}) — {} генерации видео",
                                              batch_id, target, 'возобновление' if resumed else 'начало'), level='silent')
            elif cnt == 0:
                db_log_update(log_id, f"Генерация видео ({model_name})", 'running')

            current_batch = db_get_batch_by_id(batch_id)
            current_data  = (current_batch.get('data') or {}) if current_batch else {}
            saved_request_id   = current_data.get('request_id')
            saved_status_url   = current_data.get('status_url')
            saved_response_url = current_data.get('response_url')
            client = _video_client(m.get('platform_name', ''))

            if saved_request_id:
                write_log_entry(log_id, "Возобновление предыдущего запроса.")
                write_log_entry(log_id, fmt_id_msg("Возобновление: request_id={}", saved_request_id), level='silent')
                write_log_entry(
                    log_id,
                    fmt_id_msg(
                        "[video] Батч {} — phase=poll_resume_start, model={}, request_id={}, status_url={}, response_url={}",
                        batch_id, model_name, saved_request_id, saved_status_url, saved_response_url,
                    ),
                    level='silent',
                )
                video_url, poll_err = client.poll(log_id, saved_status_url, saved_response_url)
                if video_url:
                    used_model_id = str(m['id'])
                    write_log_entry(
                        log_id,
                        fmt_id_msg(
                            "[video] Батч {} — phase=poll_resume_success, model={}, request_id={}",
                            batch_id, model_name, saved_request_id,
                        ),
                        level='silent',
                    )
                    return video_url
                if poll_err:
                    write_log_entry(log_id, f"Поллинг завершился с ошибкой: {poll_err}", level='warn')
                    write_log_entry(
                        log_id,
                        fmt_id_msg(
                            "[video] Батч {} — phase=poll_resume_error, model={}, request_id={}, error={}",
                            batch_id, model_name, saved_request_id, poll_err,
                        ),
                        level='silent',
                    )
                    if _is_content_moderation_error(poll_err):
                        msg = 'Видео отклонено модерацией контента провайдера'
                        db_log_update(log_id, msg, 'error')
                        write_log_entry(log_id, msg, level='error')
                        write_log_entry(log_id, f"[video] {msg}", level='silent')
                        raise AppException(batch_id, 'video', msg, log_id)
                else:
                    write_log_entry(
                        log_id,
                        fmt_id_msg(
                            "[video] Батч {} — phase=poll_resume_no_result, model={}, request_id={}, error=None",
                            batch_id, model_name, saved_request_id,
                        ),
                        level='silent',
                    )
                write_log_entry(log_id, f"Поллинг не дал результата, сброс handshake", level='warn')
                write_log_entry(
                    log_id,
                    fmt_id_msg("[video] Батч {} — phase=handshake_reset_after_resume, model={}, request_id={}", batch_id, model_name, saved_request_id),
                    level='silent',
                )
                db_save_video_job_and_set_pending(batch_id, {
                    'request_id':   None,
                    'status_url':   None,
                    'response_url': None,
                })
                return None

            if cnt == 0:
                db_update_batch_current_movie_model_id(batch_id, m['id'])
                write_log_entry(log_id, "Отправляю запрос к провайдеру.")
                write_log_entry(log_id, f"[video] Запрос к провайдеру: модель={model_name}, ar={ar_x}:{ar_y}", level='silent')
            else:
                write_log_entry(log_id, f"[{model_name}] попытка {cnt + 1}/{max_attempts_per_model}", level='warn')

            allowed = m.get('allowed_durations') or [0]
            actual_duration = nearest_allowed_duration(video_duration, allowed)
            if actual_duration != video_duration:
                write_log_entry(log_id, f"Длительность скорректирована: {video_duration}с → {actual_duration}с (модель поддерживает: {allowed})", level='warn')

            sr = client.submit(
                log_id, model_name, m['submit_url'], m['platform_url'],
                m['body_tpl'], story_text, ar_x, ar_y, actual_duration,
            )
            if not sr:
                write_log_entry(log_id, "Провайдер не принял запрос — переход к следующей попытке.", level='warn')
                write_log_entry(
                    log_id,
                    fmt_id_msg(
                        "[video] Батч {} — phase=submit_failed, model={}, attempt={}, submit_url={}",
                        batch_id, model_name, cnt + 1, m.get('submit_url'),
                    ),
                    level='silent',
                )
                return None

            req_id = sr['request_id']
            # status_url и response_url получены от провайдера в ответ на submit и могут
            # отличаться от шаблонного паттерна (platform_url/requests/.), поэтому
            # хранятся явно в batch.data, а не восстанавливаются из шаблона при возобновлении.
            s_url  = sr['status_url']
            r_url  = sr['response_url']
            m_id   = str(m['id'])

            db_save_video_job_and_set_pending(batch_id, {
                'request_id':   req_id,
                'status_url':   s_url,
                'response_url': r_url,
            })
            write_log_entry(log_id, "Запрос принят, ожидаю результат.")
            write_log_entry(log_id, fmt_id_msg("[video] Генерация запущена: request_id={}", req_id), level='silent')
            write_log_entry(
                log_id,
                fmt_id_msg(
                    "[video] Батч {} — phase=submit_ok, model={}, request_id={}, status_url={}, response_url={}",
                    batch_id, model_name, req_id, s_url, r_url,
                ),
                level='silent',
            )

            video_url, poll_err = client.poll(log_id, s_url, r_url)
            if video_url:
                used_model    = model_name
                used_model_id = m_id
                write_log_entry(
                    log_id,
                    fmt_id_msg(
                        "[video] Батч {} — phase=poll_submit_success, model={}, request_id={}",
                        batch_id, model_name, req_id,
                    ),
                    level='silent',
                )
                return video_url

            if poll_err:
                write_log_entry(log_id, f"Поллинг завершился с ошибкой: {poll_err}", level='warn')
                write_log_entry(
                    log_id,
                    fmt_id_msg(
                        "[video] Батч {} — phase=poll_submit_error, model={}, request_id={}, error={}",
                        batch_id, model_name, req_id, poll_err,
                    ),
                    level='silent',
                )
                if _is_content_moderation_error(poll_err):
                    msg = 'Видео отклонено модерацией контента провайдера'
                    db_log_update(log_id, msg, 'error')
                    write_log_entry(log_id, msg, level='error')
                    write_log_entry(log_id, f"[video] {msg}", level='silent')
                    raise AppException(batch_id, 'video', msg, log_id)
            else:
                write_log_entry(
                    log_id,
                    fmt_id_msg(
                        "[video] Батч {} — phase=poll_submit_no_result, model={}, request_id={}, error=None",
                        batch_id, model_name, req_id,
                    ),
                    level='silent',
                )
            write_log_entry(log_id, f"Поллинг не дал результата, сброс handshake", level='warn')
            write_log_entry(
                log_id,
                fmt_id_msg("[video] Батч {} — phase=handshake_reset_after_submit, model={}, request_id={}", batch_id, model_name, req_id),
                level='silent',
            )
            db_save_video_job_and_set_pending(batch_id, {
                'request_id':   None,
                'status_url':   None,
                'response_url': None,
            })
            return None

        # Подхват при переборе моделей (slot/adhoc, not pinned):
        # current_movie_model_id в batches.data хранит модель, на которой
        # остановились. При подхвате первый проход начинается с неё, а не с
        # начала списка — чтобы не повторять уже провалившиеся модели.
        # Второй и последующие проходы идут по полному списку с самого начала:
        # передаём полный models в iterate_models, но _orig_video_callback
        # молча пропускает (None) модели до resume_model_id без инкремента
        # attempt_counts, так что счётчики попыток этих моделей остаются чистыми
        # для следующих проходов.
        resume_model_id = (
            (batch_data.get('current_movie_model_id') if isinstance(batch_data, dict) else None)
            if resumed and not pinned_model_id else None
        )
        resume_unlocked = not resume_model_id
        if resume_model_id:
            idx = next((i for i, m in enumerate(models) if str(m['id']) == resume_model_id), None)
            if idx is not None and idx > 0:
                write_log_entry(log_id, "Подхват при возобновлении.")
                write_log_entry(log_id, fmt_id_msg("[video] Подхват: первый проход начинается с модели {} (позиция {} из {})", resume_model_id, idx + 1, len(models)), level='silent')
            else:
                resume_unlocked = True

        def _orig_video_callback(m):
            nonlocal resume_unlocked
            if not resume_unlocked:
                if str(m['id']) == resume_model_id:
                    resume_unlocked = True
                else:
                    return None
            return video_callback(m)

        cb = _orig_video_callback if resume_model_id else video_callback
        # pinned: один проход (модель задана жёстко, повторять нет смысла).
        # перебор: max_model_passes проходов по всему списку.
        max_passes = 1 if pinned_model_id else snap.max_model_passes
        video_url = iterate_models(models, max_attempts_per_model, cb, max_passes=max_passes)

        if not video_url:
            if is_manual:
                msg = f'Ручная модель {models[0]["name"]} не дала результата ({max_attempts_per_model} попыток)'
                db_log_update(log_id, msg, 'error')
                write_log_entry(log_id, msg, level='error')
                write_log_entry(log_id, f"[video] {msg}", level='silent')
                raise AppException(batch_id, 'video', msg, log_id)
            else:
                msg = f'Все активные видео-модели не дали результата после {max_passes} проходов'
                db_log_update(log_id, msg, 'error')
                write_log_entry(log_id, msg, level='error')
                write_log_entry(log_id, f"[video] {msg}", level='silent')
                write_log_entry(
                    log_id,
                    fmt_id_msg("[video] Батч {} — phase=models_exhausted, action=ready", batch_id),
                    level='silent',
                )
                db_set_batch_status(batch_id, 'ready')
                return

        write_log_entry(log_id, 'Скачиваю оригинал от провайдера.')
        write_log_entry(log_id, f"[video] Скачиваю оригинал: {video_url}", level='silent')
        try:
            original_data = falai.download_video(log_id, video_url)
            orig_mb = round(len(original_data) / 1024 / 1024, 1)
            db_create_batch_movie(batch_id, original_data, video_url, used_model_id)
            write_log_entry(log_id, "Оригинал сохранён в базе данных.")
            write_log_entry(log_id, f"[video] Оригинал сохранён в БД ({orig_mb} МБ)", level='silent')
        except Exception as e:
            msg = f"Ошибка скачивания оригинала: {e}"
            db_log_update(log_id, msg, 'error')
            write_log_entry(log_id, msg, level='error')
            write_log_entry(log_id, f"[video] {msg}", level='silent')
            raise AppException(batch_id, 'video', msg, log_id)

        db_set_batch_status(batch_id, 'ready')
        msg = f'Видео сгенерировано ({used_model})' if used_model else 'Видео сгенерировано'
        db_log_update(log_id, msg, 'ok')
        write_log_entry(log_id, 'Готово: batch → ready')
        write_log_entry(log_id, f"[video] URL: {video_url}", level='silent')
        write_log_entry(
            log_id,
            fmt_id_msg("[video] Батч {} — phase=run_done, status=ready, model_id={}", batch_id, used_model_id),
            level='silent',
        )

    except ProviderFatalError as e:
        msg = f"Фатальная ошибка провайдера: {e}"
        db_log_update(log_id, msg, 'error')
        write_log_entry(log_id, msg, level='error')
        write_log_entry(log_id, f"[video] {msg}", level='silent')
        raise AppException(batch_id, 'video', msg, log_id)
