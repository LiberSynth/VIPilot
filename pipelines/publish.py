"""
Pipeline 5 — Публикация.
Берёт первый transcode_ready-батч и публикует видео на платформу таргета.
Поддерживается: VKontakte (история + стена, настраивается через settings).
Статус: transcode_ready → published / publish_error.
"""

from datetime import datetime, timezone

import os

from db import (
    db_get,
    db_get_transcode_ready_batch,
    db_is_batch_scheduled,
    db_set_batch_obsolete,
    db_set_batch_published,
    db_set_batch_publish_error,
    db_set_batch_video_ready,
)
from log import db_log_pipeline, db_log_entry, db_log_update, db_log_interrupt_running
from clients import vk


def _publish_vk(batch, log_id):
    """Публикует батч на ВКонтакте. Возвращает True если хоть один канал успешен."""
    if not vk.is_configured():
        if log_id:
            db_log_entry(log_id, 'VK_USER_TOKEN не задан', level='error')
        return False

    group_id   = int(db_get('vk_group_id', '236929597'))
    do_story   = db_get('vk_publish_story', '1') == '1'
    do_wall    = db_get('vk_publish_wall',  '1') == '1'
    video_file = batch['video_file']

    story_ok = wall_ok = False

    if do_story:
        if log_id:
            db_log_entry(log_id, 'Публикую историю…')
        story_ok = vk.publish_story(video_file, group_id, log_id) is not None

    if do_wall:
        if log_id:
            db_log_entry(log_id, 'Публикую на стену…')
        wall_ok = vk.publish_wall(video_file, group_id, log_id) is not None

    return story_ok or wall_ok


def run():
    try:
        db_log_interrupt_running('publish')

        batch = db_get_transcode_ready_batch()
        if not batch:
            return

        batch_id = str(batch['id'])
        target   = batch['target_name']

        if not db_is_batch_scheduled(batch['scheduled_at'], batch['target_id']):
            db_set_batch_obsolete(batch_id)
            db_log_pipeline('publish', 'Батч устарел — слот удалён из расписания или таргет отключён',
                            status='прервана', batch_id=batch_id)
            print(f"[publish] Батч {batch_id[:8]}… устарел, пропускаю")
            return

        now = datetime.now(timezone.utc)
        if now < batch['scheduled_at']:
            remaining = int((batch['scheduled_at'] - now).total_seconds() / 60)
            print(f"[publish] Батч {batch_id[:8]}… ещё не время ({remaining} мин до публикации)")
            return

        print(f"[publish] Батч {batch_id[:8]}… ({target}) — начало публикации")

        log_id = db_log_pipeline(
            'publish', f'Публикация ({target})…',
            status='running', batch_id=batch_id,
        )

        if target == 'VKontakte':
            ok = _publish_vk(batch, log_id)
        else:
            msg = f'Платформа «{target}» не поддерживается'
            db_log_update(log_id, msg, 'error')
            if log_id:
                db_log_entry(log_id, msg, level='error')
            db_set_batch_publish_error(batch_id)
            print(f"[publish] {msg}")
            return

        if ok:
            db_set_batch_published(batch_id)
            db_log_update(log_id, 'Опубликовано', 'ok')
            print(f"[publish] Батч {batch_id[:8]}… опубликован")
        else:
            db_set_batch_publish_error(batch_id)
            db_log_update(log_id, 'Ошибка публикации', 'error')
            print(f"[publish] Батч {batch_id[:8]}… ошибка публикации")

    except Exception as e:
        db_log_pipeline('publish', f'Сбой пайплайна: {e}', status='error')
        print(f"[publish] Ошибка: {e}")
