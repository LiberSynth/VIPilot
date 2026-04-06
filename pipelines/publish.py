"""
Pipeline 5 — Публикация.
Принимает batch_id, публикует видео на платформу таргета.
Поддерживается: VKontakte (история + стена), Дзен (короткое видео).
Видео берётся из БД (video_data_transcoded).
Статусы: transcode_ready → story_posted → published / publish_error.
  Возобновление после краша: story_posted → published.
"""

from datetime import datetime, timezone

from utils.notify import notify_failure
from db import (
    db_get,
    db_get_active_targets,
    db_get_batch_by_id,
    db_get_batch_video_data,
    db_get_batch_original_video,
    db_get_story_text,
    db_is_batch_scheduled,
    db_set_batch_obsolete,
    db_set_batch_probe,
    db_set_batch_published,
    db_set_batch_publish_error,
    db_set_batch_story_posted,
    db_set_batch_fatal_error,
)
from log import db_log_pipeline, db_log_entry, db_log_update, db_get_log_entries
from clients import vk
from clients import dzen as dzen_client
from clients.dzen import DzenCsrfExpired


def _get_vk_video(batch_id, log_id):
    """Возвращает видеоданные батча (transcoded или original).
    Бросает RuntimeError если оба поля NULL (ошибка логики — батч не должен достигать
    transcode_ready без видео)."""
    video_data = db_get_batch_video_data(batch_id)
    if video_data is None:
        if log_id:
            db_log_entry(log_id, 'video_data_transcoded отсутствует — использую оригинал (video_data_original)')
        video_data = db_get_batch_original_video(batch_id)
        if video_data is None:
            msg = 'Ни video_data_transcoded, ни video_data_original не найдены в БД — ошибка логики'
            if log_id:
                db_log_entry(log_id, msg, level='error')
            raise RuntimeError(msg)
    return video_data


def _publish_vk(batch_id, log_id, target_config):
    """Публикует батч на ВКонтакте (история + стена). Возвращает True если хоть один канал успешен."""
    if not vk.is_configured():
        if log_id:
            db_log_entry(log_id, 'VK_USER_TOKEN не задан', level='error')
        return False

    video_data = _get_vk_video(batch_id, log_id)
    if video_data is None:
        return False

    group_id = int((target_config or {}).get('group_id', 236929597))
    do_story = db_get('vk_publish_story', '1') == '1'
    do_wall  = db_get('vk_publish_wall',  '1') == '1'

    story_ok = wall_ok = False

    if do_story:
        if log_id:
            db_log_entry(log_id, 'Публикую историю…')
        story_ok = vk.publish_story(video_data, group_id, log_id) is not None
        if story_ok:
            db_set_batch_story_posted(batch_id)

    if do_wall:
        if log_id:
            db_log_entry(log_id, 'Публикую на стену…')
        wall_ok = vk.publish_wall(video_data, group_id, log_id) is not None

    return story_ok or wall_ok


def _publish_dzen(batch_id, log_id, target_config):
    """Публикует батч на Дзен. Возвращает True при успехе."""
    cfg = target_config or {}

    if not dzen_client.is_configured(cfg):
        if log_id:
            db_log_entry(log_id, 'Дзен не настроен: проверьте publisher_id и CSRF-токен в настройках', level='error')
        return False

    video_data = db_get_batch_video_data(batch_id)
    if video_data is None:
        video_data = db_get_batch_original_video(batch_id)
    if video_data is None:
        if log_id:
            db_log_entry(log_id, 'Видео не найдено в БД (ни transcoded, ни original)', level='error')
        return False

    batch = db_get_batch_by_id(batch_id)
    story_id = batch.get('story_id') if batch else None
    title = ''
    if story_id:
        try:
            text = db_get_story_text(story_id)
            if text:
                title = text.split('\n')[0].strip()[:100]
        except Exception:
            pass
    if not title:
        title = 'Видео'

    try:
        return dzen_client.publish(video_data, cfg, title, log_id)
    except DzenCsrfExpired as e:
        if log_id:
            db_log_entry(log_id, f'Дзен: CSRF-токен истёк — обновите его в настройках → вкладка «Публикация»', level='error')
        raise


def _publish_vk_wall(batch_id, log_id, target_config):
    """Публикует только на стену (возобновление — история уже опубликована)."""
    if not vk.is_configured():
        if log_id:
            db_log_entry(log_id, 'VK_USER_TOKEN не задан — стена пропущена', level='warn')
        return

    do_wall = db_get('vk_publish_wall', '1') == '1'
    if not do_wall:
        return

    video_data = _get_vk_video(batch_id, log_id)
    if video_data is None:
        return

    group_id = int((target_config or {}).get('group_id', 236929597))
    if log_id:
        db_log_entry(log_id, 'Публикую на стену… (возобновление)')
    vk.publish_wall(video_data, group_id, log_id)


def run(batch_id):
    log_id = None
    try:
        batch = db_get_batch_by_id(batch_id)
        if not batch:
            return

        status = batch['status']
        if status not in ('transcode_ready', 'story_posted'):
            return

        # --- Возобновление после краша: история уже опубликована, только стена ---
        if status == 'story_posted':
            log_id = db_log_pipeline(
                'publish', 'Публикация — возобновление (стена)…',
                status='running', batch_id=batch_id,
            )
            for t in db_get_active_targets():
                if t['name'] == 'VKontakte':
                    _publish_vk_wall(batch_id, log_id, t.get('config'))
            db_set_batch_published(batch_id)
            db_log_update(log_id, 'Опубликовано (возобновление)', 'ok')
            print(f"[publish] Батч {batch_id[:8]}… опубликован (возобновление)")
            return

        # --- Стандартный путь: transcode_ready ---
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

        active_targets = db_get_active_targets()
        if not active_targets:
            msg = 'Нет активных таргетов — публикация невозможна'
            log_id = db_log_pipeline('publish', msg, status='error', batch_id=batch_id)
            db_set_batch_publish_error(batch_id)
            print(f"[publish] {msg}")
            notify_failure(f"publish: {msg} (батч {batch_id[:8]})")
            return

        target_names = ', '.join(t['name'] for t in active_targets)
        print(f"[publish] Батч {batch_id[:8]}… ({target_names}) — начало публикации")

        log_id = db_log_pipeline(
            'publish', f'Публикация ({target_names})…',
            status='running', batch_id=batch_id,
        )

        results = []
        for t in active_targets:
            name = t['name']
            cfg  = t.get('config') or {}
            if name == 'VKontakte':
                t_ok = _publish_vk(batch_id, log_id, cfg)
            elif name == 'Дзен':
                t_ok = _publish_dzen(batch_id, log_id, cfg)
            else:
                db_log_entry(log_id, f'Платформа «{name}» не поддерживается', level='warn')
                t_ok = False
            results.append(t_ok)

        ok = any(results)

        if ok:
            db_set_batch_published(batch_id)
            db_log_update(log_id, f'Опубликовано ({target_names})', 'ok')
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
            notify_failure(f"publish: ошибка публикации батча {batch_id[:8]} ({target_names})")

    except Exception as e:
        msg = f"Критическая ошибка приложения: {e}"
        db_set_batch_fatal_error(batch_id)
        if log_id:
            db_log_update(log_id, msg, 'error')
            db_log_entry(log_id, msg, level='error')
        else:
            new_log_id = db_log_pipeline('publish', msg, status='error', batch_id=batch_id)
            if new_log_id:
                db_log_entry(new_log_id, msg, level='error')
        print(f"[publish] Ошибка: {e}")
        notify_failure(f"publish: критическая ошибка — {e}")
