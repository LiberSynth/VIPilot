"""
Pipeline 6 — Публикация.
Принимает batch_id, публикует видео на все активные платформы.
"""

from datetime import datetime, timezone

import common.environment as environment
from utils.notify import notify_failure
from db import (
    db_get_active_targets,
    db_get_batch_by_id,
    db_get_batch_video_data,
    db_get_batch_original_video,
    db_set_batch_status,
    db_claim_batch_status,
    db_set_batch_title,
)
from log import db_log_update, db_get_log_entries, write_log_entry
from pipelines.base import check_cancelled
from common.exceptions import AppException
from utils.utils import fmt_id_msg
from routes.api import client_is_configured, build_publication_title
from clients import vk
from clients import dzen as dzen_client
from clients.dzen import DzenCsrfExpired, DzenSessionMissing
from clients import rutube as rutube_client
from clients.rutube import RutubeCsrfExpired, RutubeSessionMissing
from clients import vkvideo as vkvideo_client
from clients.vkvideo import VkVideoCsrfExpired, VkVideoSessionMissing


def is_scheduled(batch) -> bool:
    """Возвращает True если батч ещё не наступило время публиковать."""
    sched = batch.get('scheduled_at')
    if sched is None:
        return False
    if sched.tzinfo is None:
        sched = sched.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) < sched


def _get_video(batch_id, log_id):
    """Возвращает видеоданные батча (transcoded или original).
    Бросает RuntimeError если оба поля NULL."""
    video_data = db_get_batch_video_data(batch_id)
    if video_data is None:
        write_log_entry(log_id, 'movies.transcoded_data отсутствует — использую оригинал (movies.raw_data)')
        video_data = db_get_batch_original_video(batch_id)
        if video_data is None:
            msg = 'Ни movies.transcoded_data, ни movies.raw_data не найдены в БД — ошибка логики'
            write_log_entry(log_id, msg, level='error')
            raise RuntimeError(msg)
    return video_data



def _call_vk(slug, method, batch_id, log_id, target, step_results, pub_title):
    cfg = target.get('config') or {}
    if not client_is_configured('vk'):
        write_log_entry(log_id, 'VK_USER_TOKEN не задан', level='error')
        return False
    group_id = int(cfg.get('group_id', 236929597))
    if method == 'story':
        write_log_entry(log_id, 'Публикую историю.')
        write_log_entry(log_id, f"[publish] group_id={group_id}", level='silent')
        video_data = _get_video(batch_id, log_id)
        return vk.publish_story(video_data, group_id, log_id, pub_title) is not None
    elif method == 'wall':
        write_log_entry(log_id, 'Публикую на стену.')
        write_log_entry(log_id, f"[publish] group_id={group_id}", level='silent')
        video_data = _get_video(batch_id, log_id)
        return vk.publish_wall(video_data, group_id, log_id, pub_title) is not None
    elif method == 'clip_wall':
        vkvideo_result = step_results.get('vkvideo') or {}
        clip_url = vkvideo_result.get('clip_url', '')
        if not clip_url:
            write_log_entry(log_id, 'VK clip_wall: clip_url не получен от шага vkvideo — пропуск', level='error')
            return False
        write_log_entry(log_id, f'VK: Публикую пост с клипом VK Видео.')
        write_log_entry(log_id, f"[publish] clip_url={clip_url}", level='silent')
        return vk.publish_clip_wall(clip_url, pub_title, group_id, log_id) is not None
    else:
        write_log_entry(log_id, f'VK: неизвестный метод «{method}» — пропуск', level='warn')
        return False


def _call_dzen(slug, method, batch_id, log_id, target, step_results, pub_title):
    cfg = target.get('config') or {}
    target_id = target.get('id')
    if not client_is_configured('dzen', cfg, target_id):
        if not cfg.get('publisher_id'):
            write_log_entry(log_id, 'Дзен не настроен: publisher_id отсутствует', level='error')
        else:
            write_log_entry(log_id, 'Дзен: браузерная сессия не сохранена — авторизуйтесь на вкладке «Публикация»', level='error')
        return False

    video_data = _get_video(batch_id, log_id)

    return dzen_client.publish(video_data, cfg, log_id, batch_id=batch_id, target_id=target_id, pub_title=pub_title)


def _call_rutube(slug, method, batch_id, log_id, target, step_results, pub_title):
    cfg = target.get('config') or {}
    target_id = target.get('id')
    if not client_is_configured('rutube', cfg, target_id):
        if not cfg.get('person_id'):
            write_log_entry(log_id, 'Рутьюб не настроен: person_id отсутствует', level='error')
        else:
            write_log_entry(log_id, 'Рутьюб: браузерная сессия не сохранена — авторизуйтесь на вкладке «Публикация»', level='error')
        return False

    video_data = _get_video(batch_id, log_id)

    return rutube_client.publish(video_data, cfg, log_id, batch_id=batch_id, target_id=target_id, pub_title=pub_title)


def _call_vkvideo(slug, method, batch_id, log_id, target, step_results, pub_title):
    cfg = target.get('config') or {}
    target_id = target.get('id')
    if not client_is_configured('vkvideo', cfg, target_id):
        if not cfg.get('club_id'):
            write_log_entry(log_id, 'VK Видео не настроен: club_id отсутствует', level='error')
        else:
            write_log_entry(log_id, 'VK Видео: браузерная сессия не сохранена — авторизуйтесь на вкладке «Публикация»', level='error')
        return False

    video_data = _get_video(batch_id, log_id)

    result = vkvideo_client.publish(video_data, cfg, log_id, batch_id=batch_id, target_id=target_id, pub_title=pub_title)
    step_results['vkvideo'] = {
        'clip_url': result.get('clip_url', ''),
    }
    return result.get('ok', False)


_CLIENTS = {
    'vk':      _call_vk,
    'dzen':    _call_dzen,
    'rutube':  _call_rutube,
    'vkvideo': _call_vkvideo,
}


def _call_client(slug, method, batch_id, log_id, target, step_results, pub_title):
    """Диспетчеризует вызов клиента по реестру _CLIENTS. Возвращает True при успехе."""
    handler = _CLIENTS.get(slug)
    if handler is None:
        write_log_entry(log_id, f'Платформа «{slug}» не поддерживается', level='warn')
        return False
    return handler(slug, method, batch_id, log_id, target, step_results, pub_title)


def _parse_composite_status(status: str):
    """Парсит составной статус вида slug.method.phase → (slug, method, phase) или None."""
    parts = status.split('.')
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    return None


def _build_steps(active_targets):
    """Строит список шагов публикации: [(slug, method, target), ...] по алфавиту метода внутри таргета."""
    steps = []
    for t in active_targets:
        slug = t.get('slug') or ''
        if not slug:
            continue
        cfg = t.get('config') or {}
        methods = cfg.get('publish_method') or {}
        enabled_methods = sorted(k for k, v in methods.items() if str(v).strip() not in ('0', '', 'false', 'False'))
        for method in enabled_methods:
            steps.append((slug, method, t))
    return steps


def run(batch_id, log_id):
    snap = environment.snapshot()
    batch = db_get_batch_by_id(batch_id)
    if not batch:
        return

    status = batch['status']
    active_targets = db_get_active_targets()

    parsed = _parse_composite_status(status)

    if parsed is None and status != 'transcode_ready':
        return

    if parsed is None:
        if batch['type'] == 'movie_probe':
            db_log_update(log_id, 'Публикация (пробный).', 'running')
            write_log_entry(log_id, 'Пробный батч — публикация на платформу не выполняется')
            write_log_entry(log_id, fmt_id_msg("[publish] Батч {} пробный — публикация пропущена", batch_id), level='silent')
            db_set_batch_status(batch_id, 'movie_probe')
            db_log_update(log_id, 'Без публикации (пробный батч)', 'ok')
            return

        if check_cancelled('publish', batch_id, batch, log_id):
            return

        now = datetime.now(timezone.utc)
        if batch['scheduled_at'] is not None and now < batch['scheduled_at']:
            remaining = int((batch['scheduled_at'] - now).total_seconds() / 60)
            write_log_entry(log_id, fmt_id_msg("Ещё {} мин до публикации", remaining))
            write_log_entry(log_id, fmt_id_msg("[publish] Батч {} ещё не время ({} мин до публикации)", batch_id, remaining), level='silent')
            return

    if not active_targets:
        if parsed is None:
            msg = 'Батч отменён — нет активных таргетов'
            db_set_batch_status(batch_id, 'cancelled')
            db_log_update(log_id, msg, 'cancelled')
            write_log_entry(log_id, msg)
            write_log_entry(log_id, fmt_id_msg("[publish] {} (батч {})", msg, batch_id), level='silent')
            return
        else:
            msg = 'Нет активных таргетов — публикация невозможна'
            db_log_update(log_id, msg, 'error')
            write_log_entry(log_id, msg, level='error')
            write_log_entry(log_id, f"[publish] {msg}", level='silent')
            raise AppException(batch_id, 'publish', msg, log_id)

    steps = _build_steps(active_targets)
    if not steps:
        if parsed is None:
            msg = 'Батч отменён — нет методов публикации в конфиге таргетов'
            db_set_batch_status(batch_id, 'cancelled')
            db_log_update(log_id, msg, 'cancelled')
            write_log_entry(log_id, msg)
            write_log_entry(log_id, fmt_id_msg("[publish] {} (батч {})", msg, batch_id), level='silent')
            return
        else:
            msg = 'Нет методов публикации в конфиге таргетов'
            db_log_update(log_id, msg, 'error')
            write_log_entry(log_id, msg, level='error')
            write_log_entry(log_id, f"[publish] {msg}", level='silent')
            raise AppException(batch_id, 'publish', msg, log_id)

    # Проверяем зависимость: vk.clip_wall требует, чтобы vkvideo-таргет
    # был раньше по списку шагов — иначе step_results['vkvideo'] будет пустым.
    _vkvideo_positions = {i for i, (s, m, _) in enumerate(steps) if s == 'vkvideo'}
    for _i, (_s, _m, _) in enumerate(steps):
        if _s == 'vk' and _m == 'clip_wall':
            if not any(vi < _i for vi in _vkvideo_positions):
                _msg = ('vk.clip_wall зависит от таргета vkvideo, '
                        'но vkvideo отсутствует или стоит позже в списке таргетов — '
                        'исправьте порядок таргетов в настройках')
                write_log_entry(log_id, _msg, level='error')
                write_log_entry(log_id, fmt_id_msg("[publish] Батч {}: {}", batch_id, _msg), level='silent')
                db_log_update(log_id, _msg, 'error')
                raise AppException(batch_id, 'publish', _msg, log_id)

    target_names = ', '.join(t['name'] for t in active_targets)

    if parsed is not None:
        cur_slug, cur_method, cur_phase = parsed
        log_label = f'Публикация (возобновление {cur_slug}.{cur_method}).'
    else:
        log_label = f'Публикация ({target_names}).'

    db_log_update(log_id, log_label, 'running')

    resume_from = None
    if parsed is not None:
        cur_slug, cur_method, cur_phase = parsed
        if cur_phase == 'pending':
            step_found = any(slug == cur_slug and method == cur_method for slug, method, _ in steps)
            if step_found:
                resume_from = (cur_slug, cur_method)
                write_log_entry(log_id, fmt_id_msg("Возобновление публикации с шага {}.{}", cur_slug, cur_method))
                write_log_entry(log_id, fmt_id_msg("[publish] Батч {} resume с шага {}.{}", batch_id, cur_slug, cur_method), level='silent')
            else:
                msg = f'Шаг {cur_slug}.{cur_method} удалён из конфига — батч требует ручной проверки'
                write_log_entry(log_id, f'Шаг {cur_slug}.{cur_method} более не активен в конфиге — перевожу в ошибку', level='error')
                write_log_entry(log_id, fmt_id_msg("[publish] Батч {} шаг {}.{} не найден в текущем конфиге", batch_id, cur_slug, cur_method), level='silent')
                db_log_update(log_id, msg, 'error')
                raise AppException(batch_id, 'publish', msg, log_id)
        elif cur_phase in ('published', 'failed'):
            found_idx = None
            for i, (slug, method, _) in enumerate(steps):
                if slug == cur_slug and method == cur_method:
                    found_idx = i
                    break

            if found_idx is None:
                msg = f'Шаг {cur_slug}.{cur_method} удалён из конфига — батч требует ручной проверки'
                write_log_entry(log_id, msg, level='error')
                write_log_entry(log_id, fmt_id_msg("[publish] Батч {} шаг {}.{} не найден в текущем конфиге", batch_id, cur_slug, cur_method), level='silent')
                db_log_update(log_id, msg, 'error')
                raise AppException(batch_id, 'publish', msg, log_id)
            elif found_idx + 1 < len(steps):
                ns, nm, _ = steps[found_idx + 1]
                resume_from = (ns, nm)
                write_log_entry(log_id, fmt_id_msg("Продолжаю с шага {}.{}", ns, nm))
                write_log_entry(log_id, fmt_id_msg("[publish] Батч {} resume с шага {}.{}", batch_id, ns, nm), level='silent')
            else:
                db_set_batch_status(batch_id, 'published')
                db_log_update(log_id, 'Опубликовано (возобновление — все шаги завершены)', 'ok')
                write_log_entry(log_id, 'Опубликовано (возобновление — все шаги завершены)')
                write_log_entry(log_id, fmt_id_msg("[publish] Батч {} опубликован (возобновление)", batch_id), level='silent')
                return

    pub_title = batch.get('title') or ''
    if pub_title:
        write_log_entry(log_id, f"[publish] Заголовок публикации (из БД): {pub_title}", level='silent')

    any_ok = False
    failed_steps = []
    expected_from = status
    step_results = {}  # передаётся между шагами: vkvideo → vk (clip_url)
    for step_idx, (slug, method, target) in enumerate(steps):
        if resume_from is not None:
            if (slug, method) != resume_from:
                continue
            resume_from = None

        posting_status = f"{slug}.{method}.posting"
        published_status = f"{slug}.{method}.published"
        failed_status = f"{slug}.{method}.failed"

        if not db_claim_batch_status(batch_id, expected_from, posting_status):
            write_log_entry(log_id, 'Батч уже захвачен другим процессом — пропуск')
            write_log_entry(log_id, fmt_id_msg("[publish] Батч {} уже захвачен другим процессом для {} — пропуск", batch_id, posting_status), level='silent')
            db_log_update(log_id, f'Захват {posting_status} не удался — пропуск', 'warn')
            return

        if not pub_title:
            pub_title = build_publication_title()
            write_log_entry(log_id, f"[publish] Заголовок публикации (новый): {pub_title}", level='silent')

        write_log_entry(log_id, f'Шаг {slug}.{method}: выполняю.')
        write_log_entry(log_id, fmt_id_msg("[publish] Батч {}: шаг {}.{} — начало", batch_id, slug, method), level='silent')

        step_error = None
        try:
            ok = _call_client(slug, method, batch_id, log_id, target, step_results, pub_title)
        except (DzenSessionMissing, DzenCsrfExpired, RutubeSessionMissing, RutubeCsrfExpired, VkVideoSessionMissing, VkVideoCsrfExpired) as e:
            ok = False
            step_error = str(e)
            write_log_entry(log_id, f'{slug}.{method}: {e}', level='error')
        except Exception as e:
            ok = False
            step_error = str(e)
            write_log_entry(log_id, f'{slug}.{method}: неожиданная ошибка: {e}', level='error')

        if not ok:
            failed_steps.append(f"{slug}.{method}")
            err_msg = step_error or 'ошибка публикации'
            write_log_entry(log_id, fmt_id_msg("[publish] Батч {} ошибка {}.{}: {}", batch_id, slug, method, err_msg), level='silent')
            db_set_batch_status(batch_id, failed_status)
            expected_from = failed_status
            continue

        any_ok = True

        db_set_batch_status(batch_id, published_status)
        write_log_entry(log_id, f'Шаг {slug}.{method}: опубликовано')
        write_log_entry(log_id, fmt_id_msg("[publish] Батч {}: шаг {}.{} — завершено успешно", batch_id, slug, method), level='silent')
        expected_from = published_status

    if any_ok:
        db_set_batch_title(batch_id, pub_title)
        if failed_steps:
            fail_list = ', '.join(failed_steps)
            db_set_batch_status(batch_id, 'published_partially')
            db_log_update(log_id, f'Частично опубликовано ({target_names}); ошибки: {fail_list}', 'ok')
            write_log_entry(log_id, f'Частично опубликовано; ошибки: {fail_list}')
            write_log_entry(log_id, fmt_id_msg("[publish] Батч {} частично опубликован (ошибки: {})", batch_id, fail_list), level='silent')
            entries = db_get_log_entries(log_id) if log_id else []
            err_entries = [e for e in entries if e['level'] in ('warn', 'error')]
            notify_failure(
                fmt_id_msg("publish: батч {} частично опубликован; не удалось: {}", batch_id, fail_list),
                log_entries=err_entries,
                partial=True,
            )
        else:
            db_set_batch_status(batch_id, 'published')
            db_log_update(log_id, f'Опубликовано ({target_names})', 'ok')
            write_log_entry(log_id, f'Успешно опубликовано ({target_names})')
            write_log_entry(log_id, fmt_id_msg("[publish] Батч {} опубликован", batch_id), level='silent')
            entries = db_get_log_entries(log_id)
            warn_entries = [e for e in entries if e['level'] in ('warn', 'error')]
            if warn_entries:
                notify_failure(
                    fmt_id_msg("publish: батч {} опубликован, но в процессе были некритичные ошибки", batch_id),
                    log_entries=warn_entries,
                    partial=True,
                )
    else:
        abt_msg = f'Ошибка публикации батча в {target_names}'
        db_log_update(log_id, 'Ошибка публикации', 'error')
        write_log_entry(log_id, f'Ошибка публикации батча в {target_names}', level='error')
        write_log_entry(log_id, fmt_id_msg("[publish] Батч {} ошибка публикации", batch_id), level='silent')
        raise AppException(batch_id, 'publish', abt_msg, log_id)
