"""
Pipeline 5 — Публикация.
Берёт первый transcode_ready-батч и публикует видео на платформу таргета.
Поддерживается: VKontakte (история + стена, настраивается через settings).
Видео берётся из БД (video_data_transcoded). Статус: transcode_ready → published / publish_error.
"""

from datetime import datetime, timezone

from utils.notify import notify_failure
from db import (
    db_get,
    db_get_transcode_ready_batch,
    db_get_batch_video_data,
    db_get_batch_original_video,
    db_is_batch_scheduled,
    db_set_batch_obsolete,
    db_set_batch_probe,
    db_set_batch_published,
    db_set_batch_publish_error,
)
from log import db_log_pipeline, db_log_entry, db_log_update, db_log_interrupt_running, db_get_log_entries
from clients import vk


def _publish_vk(batch_id, log_id):
    """Публикует батч на ВКонтакте. Возвращает True если хоть один канал успешен."""
    if not vk.is_configured():
        if log_id:
            db_log_entry(log_id, 'VK_USER_TOKEN не задан', level='error')
        return False

    video_data = db_get_batch_video_data(batch_id)
    if video_data is None:
        if log_id:
            db_log_entry(log_id, 'video_data_transcoded отсутствует — использую оригинал (video_data_original)')
        video_data = db_get_batch_original_video(batch_id)
        if video_data is None:
            if log_id:
                db_log_entry(log_id, 'Ни video_data_transcoded, ни video_data_original не найдены в БД', level='error')
            return False

    group_id = int(db_get('vk_group_id', '236929597'))
    do_story  = db_get('vk_publish_story', '1') == '1'
    do_wall   = db_get('vk_publish_wall',  '1') == '1'

    story_ok = wall_ok = False

    if do_story:
        if log_id:
            db_log_entry(log_id, 'Публикую историю…')
        story_ok = vk.publish_story(video_data, group_id, log_id) is not None

    if do_wall:
        if log_id:
            db_log_entry(log_id, 'Публикую на стену…')
        wall_ok = vk.publish_wall(video_data, group_id, log_id) is not None

    return story_ok or wall_ok


def run():
    batch_id = None
    try:
        db_log_interrupt_running('publish')

        batch = db_get_transcode_ready_batch()
        if not batch:
            return

        batch_id = str(batch['id'])
        target   = batch['target_name'] or 'пробный'

        # Пробный батч без таргета — публикация не нужна, переводим в probe
        if batch['target_id'] is None:
            log_id = db_log_pipeline('publish', 'Публикация (пробный)…',
                                     status='running', batch_id=batch_id)
            db_log_entry(log_id, 'Таргет не назначен — публикация на платформу не выполняется')
            db_set_batch_probe(batch_id)
            db_log_update(log_id, 'Без публикации (пробный батч)', 'ok')
            print(f"[publish] Батч {batch_id[:8]}… пробный — публикация пропущена")
            return

        if not db_is_batch_scheduled(batch['scheduled_at'], batch['target_id']):
            db_set_batch_obsolete(batch_id)
            db_log_pipeline('publish', 'Батч отменён — слот удалён из расписания или таргет отключён',
                            status='прервана', batch_id=batch_id)
            print(f"[publish] Батч {batch_id[:8]}… отменён, пропускаю")
            return

        now = datetime.now(timezone.utc)
        if batch['scheduled_at'] is not None and now < batch['scheduled_at']:
            remaining = int((batch['scheduled_at'] - now).total_seconds() / 60)
            print(f"[publish] Батч {batch_id[:8]}… ещё не время ({remaining} мин до публикации)")
            return

        print(f"[publish] Батч {batch_id[:8]}… ({target}) — начало публикации")

        log_id = db_log_pipeline(
            'publish', f'Публикация ({target})…',
            status='running', batch_id=batch_id,
        )

        if target == 'VKontakte':
            ok = _publish_vk(batch_id, log_id)
        else:
            msg = f'Платформа «{target}» не поддерживается'
            db_log_update(log_id, msg, 'error')
            if log_id:
                db_log_entry(log_id, msg, level='error')
            db_set_batch_publish_error(batch_id)
            print(f"[publish] {msg}")
            notify_failure(f"publish: {msg} (батч {batch_id[:8]})")
            return

        if ok:
            db_set_batch_published(batch_id)
            db_log_update(log_id, f'Опубликовано ({target})', 'ok')
            print(f"[publish] Батч {batch_id[:8]}… опубликован")
            if log_id:
                entries = db_get_log_entries(log_id)
                warn_entries = [e for e in entries if e['level'] in ('warn', 'error')]
                if warn_entries:
                    notify_failure(
                        f"publish: батч {batch_id[:8]} опубликован, но в процессе были некритичные ошибки",
                        log_entries=warn_entries,
                        partial=True,
                    )
        else:
            db_set_batch_publish_error(batch_id)
            db_log_update(log_id, 'Ошибка публикации', 'error')
            print(f"[publish] Батч {batch_id[:8]}… ошибка публикации")
            notify_failure(f"publish: ошибка публикации батча {batch_id[:8]} ({target})")

    except Exception as e:
        db_log_pipeline('publish', f'Сбой пайплайна: {e}', status='error',
                        batch_id=batch_id)
        print(f"[publish] Ошибка: {e}")
        notify_failure(f"сбой publish-пайплайна: {e}")
