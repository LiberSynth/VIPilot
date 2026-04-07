"""
Pipeline 5 — Публикация.
Принимает batch_id, публикует видео на платформу таргета.
Поддерживается: VKontakte (история + стена), Дзен (короткое видео).
Видео берётся из БД (video_data_transcoded).
Статусы: transcode_ready → story_posted → wall_posting → published / publish_error.
  wall_posting — промежуточный статус: атомарный захват из story_posted исключает двойной resume.
  story_posted включён в actionable (resume после краша до wall-публикации).
  wall_posting НЕ в actionable: активный статус выполнения, краш в нём требует ручного сброса.
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
    db_set_batch_probe,
    db_set_batch_published,
    db_set_batch_publish_error,
    db_set_batch_story_posted,
    db_claim_batch_wall_posting,
)
from log import db_log_pipeline, db_log_entry, db_log_update, db_get_log_entries
from pipelines.base import check_cancelled, handle_critical_error
from clients import vk
from clients import dzen as dzen_client
from clients.dzen import DzenCsrfExpired, DzenSessionMissing
from db import db_get_target_browser_session


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
        if story_ok:
            if not db_claim_batch_wall_posting(batch_id):
                if log_id:
                    db_log_entry(log_id, 'Батч уже захвачен другим процессом для публикации стены — этот воркер не устанавливает финальный статус', level='warn')
                return None
        if log_id:
            db_log_entry(log_id, 'Публикую на стену…')
        wall_ok = vk.publish_wall(video_data, group_id, log_id) is not None

    return story_ok or wall_ok


def _publish_dzen(batch_id, log_id, target):
    """Публикует батч на Дзен. Возвращает True при успехе."""
    cfg = target.get('config') or {} if isinstance(target, dict) else (target or {})
    target_id = target.get('id') if isinstance(target, dict) else None

    browser_session = None
    if target_id:
        browser_session = db_get_target_browser_session(target_id)

    if not dzen_client.is_configured(cfg, browser_session):
        if log_id:
            if not cfg.get('publisher_id'):
                db_log_entry(log_id, 'Дзен не настроен: publisher_id отсутствует', level='error')
            else:
                db_log_entry(log_id, 'Дзен: браузерная сессия не сохранена — авторизуйтесь на вкладке «Публикация»', level='error')
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
        return dzen_client.publish(video_data, cfg, title, log_id, browser_session=browser_session)
    except DzenSessionMissing as e:
        if log_id:
            db_log_entry(log_id, f'Дзен: {e}', level='error')
        raise
    except DzenCsrfExpired as e:
        if log_id:
            db_log_entry(log_id, f'Дзен: {e}', level='error')
        raise


def _publish_vk_wall(batch_id, log_id, target_config):
    """Публикует только на стену (возобновление — история уже опубликована).
    Возвращает True при успехе, False при любой ошибке."""
    if not vk.is_configured():
        if log_id:
            db_log_entry(log_id, 'VK_USER_TOKEN не задан — стена пропущена', level='warn')
        return False

    do_wall = db_get('vk_publish_wall', '1') == '1'
    if not do_wall:
        if log_id:
            db_log_entry(log_id, 'Публикация на стену отключена в настройках — возобновление завершено без стены')
        return True

    video_data = _get_vk_video(batch_id, log_id)
    if video_data is None:
        return False

    group_id = int((target_config or {}).get('group_id', 236929597))
    if log_id:
        db_log_entry(log_id, 'Публикую на стену… (возобновление)')
    return vk.publish_wall(video_data, group_id, log_id) is not None


def run(batch_id):
    log_id = None
    try:
        batch = db_get_batch_by_id(batch_id)
        if not batch:
            return

        status = batch['status']
        if status not in ('transcode_ready', 'story_posted', 'wall_posting'):
            return

        # --- Возобновление после краша: история уже опубликована, только стена ---
        if status in ('story_posted', 'wall_posting'):
            if status == 'story_posted':
                if not db_claim_batch_wall_posting(batch_id):
                    print(f"[publish] Батч {batch_id[:8]}… уже захвачен другим процессом (wall_posting) — пропуск")
                    return
            # wall_posting: батч уже захвачен предыдущим claim-ом или стандартным прогоном,
            # обрабатываем напрямую — краш между claim и завершением стены.

            log_id = db_log_pipeline(
                'publish', 'Публикация — возобновление (стена)…',
                status='running', batch_id=batch_id,
            )
            wall_ok = False
            for t in db_get_active_targets():
                if t['name'] == 'VKontakte':
                    wall_ok = _publish_vk_wall(batch_id, log_id, t.get('config'))

            if wall_ok:
                saved = db_set_batch_published(batch_id)
                if saved:
                    db_log_update(log_id, 'Опубликовано (возобновление)', 'ok')
                    print(f"[publish] Батч {batch_id[:8]}… опубликован (возобновление)")
                else:
                    db_set_batch_publish_error(batch_id)
                    db_log_update(log_id, 'Стена опубликована, но ошибка записи статуса в БД (возобновление)', 'error')
                    print(f"[publish] Батч {batch_id[:8]}… ошибка записи published в БД (возобновление)")
                    notify_failure(f"publish: стена опубликована, но статус не сохранён в БД — батч {batch_id[:8]} (возобновление)")
            else:
                db_set_batch_publish_error(batch_id)
                db_log_update(log_id, 'Ошибка публикации на стену (возобновление)', 'error')
                print(f"[publish] Батч {batch_id[:8]}… ошибка публикации на стену (возобновление)")
                notify_failure(f"publish: ошибка публикации на стену батча {batch_id[:8]} (возобновление)")
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

        if check_cancelled('publish', batch_id, batch):
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
        handed_off = False
        for t in active_targets:
            name = t['name']
            cfg  = t.get('config') or {}
            if name == 'VKontakte':
                t_ok = _publish_vk(batch_id, log_id, cfg)
                if t_ok is None:
                    handed_off = True
                    t_ok = False
            elif name == 'Дзен':
                t_ok = _publish_dzen(batch_id, log_id, t)
            else:
                db_log_entry(log_id, f'Платформа «{name}» не поддерживается', level='warn')
                t_ok = False
            results.append(t_ok)

        if handed_off:
            db_log_update(log_id, 'Стена VK передана другому воркеру — финальный статус установит он', 'ok')
            print(f"[publish] Батч {batch_id[:8]}… стена VK передана другому воркеру")
            return

        ok = any(results)

        if ok:
            saved = db_set_batch_published(batch_id)
            if saved:
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
                db_log_update(log_id, f'Опубликовано, но ошибка записи статуса в БД ({target_names})', 'error')
                print(f"[publish] Батч {batch_id[:8]}… ошибка записи published в БД")
                notify_failure(f"publish: опубликовано, но статус не сохранён в БД — батч {batch_id[:8]} ({target_names})")
        else:
            db_set_batch_publish_error(batch_id)
            db_log_update(log_id, 'Ошибка публикации', 'error')
            print(f"[publish] Батч {batch_id[:8]}… ошибка публикации")
            notify_failure(f"publish: ошибка публикации батча {batch_id[:8]} ({target_names})")

    except Exception as e:
        handle_critical_error('publish', batch_id, log_id, e)
