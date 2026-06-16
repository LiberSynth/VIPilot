"""
Pipeline 6 — Публикация.
Принимает batch_id, публикует видео на все активные платформы.
"""

import common.environment as environment
from utils.notify import notify_failure
from db import (
    db_get_active_targets,
    db_get_batch_by_id,
    db_get_batch_video_data_with_source,
    db_set_batch_status,
    db_claim_batch_status,
    db_set_batch_title,
    db_get_batch_vkvideo_clip_url,
    db_set_movie_published,
)
from log import db_get_batch_log_entries, write_log_entry
from pipelines.base import ensure_playwright_chromium
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
from services.publish_batch_browser import (
    PW_PUBLISH_SLUGS,
    PublishBatchBrowserSession,
    finalize_publish_batch_browser,
    pw_step_count,
)

def _get_video(batch_id, category):
    """Возвращает видеоданные батча (transcoded или original).
    Бросает RuntimeError если отсутствуют оба файла."""
    video_data, source = db_get_batch_video_data_with_source(batch_id)
    if video_data is None:
        msg = 'Ни transcoded-файл, ни raw-файл не найдены у батча — ошибка логики'
        write_log_entry(batch_id, category, msg, level='error')
        raise RuntimeError(msg)
    if source == 'raw_data':
        write_log_entry(batch_id, category, 'transcoded-файл отсутствует — использую оригинал (raw-файл)')
        write_log_entry(batch_id, category, fmt_id_msg("[publish] Батч {} — phase=video_source_fallback, source=raw_data", batch_id), level='silent')
    write_log_entry(
        batch_id, category,
        fmt_id_msg("[publish] Батч {} — phase=video_source_selected, source={}, bytes={}", batch_id, source, len(video_data)),
        level='silent',
    )
    return video_data

def _call_vk(slug, method, batch_id, category, target, pub_title):
    cfg = target.get('config') or {}
    if not client_is_configured('vk'):
        write_log_entry(batch_id, category, 'VK_USER_TOKEN не задан', level='error')
        return False
    group_id = int(cfg.get('group_id', 236929597))
    if method == 'story':
        write_log_entry(batch_id, category, 'Публикую историю.')
        write_log_entry(batch_id, category, f"group_id={group_id}", level='silent')
        video_data = _get_video(batch_id, category)
        return vk.publish_story(video_data, group_id, batch_id, category, pub_title) is not None
    elif method == 'wall':
        write_log_entry(batch_id, category, 'Публикую на стену.')
        write_log_entry(batch_id, category, f"group_id={group_id}", level='silent')
        video_data = _get_video(batch_id, category)
        return vk.publish_wall(video_data, group_id, batch_id, category, pub_title) is not None
    elif method == 'clip_wall':
        clip_url = db_get_batch_vkvideo_clip_url(batch_id)
        if not clip_url:
            write_log_entry(batch_id, category, 'VK clip_wall: clip_url не найден в БД — vkvideo не завершился или не записал ссылку', level='error')
            return False
        write_log_entry(batch_id, category, f'VK: Публикую пост с клипом VK Видео.')
        write_log_entry(batch_id, category, f"clip_url={clip_url}", level='silent')
        return vk.publish_clip_wall(clip_url, pub_title, group_id, batch_id, category) is not None
    else:
        write_log_entry(batch_id, category, f'VK: неизвестный метод «{method}» — пропуск', level='warn')
        return False

def _call_dzen(slug, method, batch_id, category, target, pub_title, batch_session=None, keep_browser=False):
    cfg = target.get('config') or {}
    target_id = target.get('id')
    if not client_is_configured('dzen', cfg, target_id):
        if not cfg.get('publisher_id'):
            write_log_entry(batch_id, category, 'Дзен не настроен: publisher_id отсутствует', level='error')
        else:
            write_log_entry(batch_id, category, 'Дзен: браузерная сессия не сохранена — авторизуйтесь на вкладке «Публикация»', level='error')
        return False

    video_data = _get_video(batch_id, category)

    return dzen_client.publish(
        video_data, cfg, batch_id, category,
        target_id=target_id, pub_title=pub_title,
        batch_session=batch_session, keep_browser=keep_browser,
    )

def _call_rutube(slug, method, batch_id, category, target, pub_title, batch_session=None, keep_browser=False):
    cfg = target.get('config') or {}
    target_id = target.get('id')
    if not client_is_configured('rutube', cfg, target_id):
        if not cfg.get('person_id'):
            write_log_entry(batch_id, category, 'Рутьюб не настроен: person_id отсутствует', level='error')
        else:
            write_log_entry(batch_id, category, 'Рутьюб: браузерная сессия не сохранена — авторизуйтесь на вкладке «Публикация»', level='error')
        return False

    video_data = _get_video(batch_id, category)

    return rutube_client.publish(
        video_data, cfg, batch_id, category,
        target_id=target_id, pub_title=pub_title,
        batch_session=batch_session, keep_browser=keep_browser,
    )

def _call_vkvideo(slug, method, batch_id, category, target, pub_title, batch_session=None, keep_browser=False):
    cfg = target.get('config') or {}
    target_id = target.get('id')
    if not client_is_configured('vkvideo', cfg, target_id):
        if not cfg.get('club_id'):
            write_log_entry(batch_id, category, 'VK Видео не настроен: club_id отсутствует', level='error')
        else:
            write_log_entry(batch_id, category, 'VK Видео: браузерная сессия не сохранена — авторизуйтесь на вкладке «Публикация»', level='error')
        return False

    video_data = _get_video(batch_id, category)

    result = vkvideo_client.publish(
        video_data, cfg, batch_id, category,
        target_id=target_id, pub_title=pub_title,
        batch_session=batch_session, keep_browser=keep_browser,
    )
    return result.get('ok', False)

_CLIENTS = {
    'vk':      _call_vk,
    'dzen':    _call_dzen,
    'rutube':  _call_rutube,
    'vkvideo': _call_vkvideo,
}

def _call_client(slug, method, batch_id, category, target, pub_title, batch_session=None, keep_browser=False):
    """Диспетчеризует вызов клиента по реестру _CLIENTS. Возвращает True при успехе."""
    handler = _CLIENTS.get(slug)
    if handler is None:
        write_log_entry(batch_id, category, f'Платформа «{slug}» не поддерживается', level='warn')
        return False
    if slug in PW_PUBLISH_SLUGS:
        return handler(
            slug, method, batch_id, category, target, pub_title,
            batch_session=batch_session, keep_browser=keep_browser,
        )
    return handler(slug, method, batch_id, category, target, pub_title)

def _parse_composite_status(status: str):
    """Парсит составной статус вида slug.method.phase → (slug, method, phase) или None."""
    parts = status.split('.')
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    return None

def _fail_publish_config(batch_id, category, msg):
    db_set_batch_status(batch_id, 'error')
    write_log_entry(batch_id, category, msg, level='error')
    write_log_entry(batch_id, category, fmt_id_msg("[publish] {} (батч {})", msg, batch_id), level='silent')

def _mark_movie_published(batch):
    movie_id = batch.get('movie_id')
    if movie_id:
        db_set_movie_published(str(movie_id))

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

def run(batch_id, category):
    snap = environment.snapshot()
    batch = db_get_batch_by_id(batch_id)
    if not batch:
        return
    if batch.get('type') == 'movie':
        write_log_entry(batch_id, category, fmt_id_msg("[publish] Батч {} type=movie — шаг пропущен", batch_id), level='silent')
        return
    if batch.get('type') != 'publish':
        write_log_entry(
            batch_id, category,
            fmt_id_msg("[publish] Батч {} type={} — шаг пропущен", batch_id, batch.get('type')),
            level='silent',
        )
        return

    status = batch['status']
    active_targets = db_get_active_targets()
    source_label = batch.get('batch_id_source') or 'NULL'
    write_log_entry(
        batch_id, category,
        fmt_id_msg(
            "[publish] Батч {} — phase=run_start, status={}, type={}, targets_count={}",
            batch_id, status, batch.get('type'), len(active_targets),
        ),
        level='silent',
    )
    write_log_entry(batch_id, category, fmt_id_msg("Передающий батч: {}", source_label))

    parsed = _parse_composite_status(status)

    if parsed is None and status != 'pending':
        return

    if parsed is None:
        if not active_targets:
            _fail_publish_config(batch_id, category, 'Нет активных таргетов — публикация невозможна')
            return
    elif not active_targets:
        msg = 'Нет активных таргетов — публикация невозможна'
        write_log_entry(batch_id, category, msg, level='error')
        write_log_entry(batch_id, category, f"{msg}", level='silent')
        raise AppException(batch_id, 'publish', msg)

    steps = _build_steps(active_targets)
    write_log_entry(
        batch_id, category,
        fmt_id_msg("[publish] Батч {} — phase=steps_built, steps_count={}", batch_id, len(steps)),
        level='silent',
    )
    if not steps:
        if parsed is None:
            _fail_publish_config(batch_id, category, 'Нет методов публикации в конфиге таргетов')
            return
        else:
            msg = 'Нет методов публикации в конфиге таргетов'
            write_log_entry(batch_id, category, msg, level='error')
            write_log_entry(batch_id, category, f"{msg}", level='silent')
            raise AppException(batch_id, 'publish', msg)

    # Проверяем зависимость: vk.clip_wall требует, чтобы vkvideo-таргет
    # был раньше по списку шагов — иначе clip_url не будет записан в БД к моменту шага.
    _vkvideo_positions = {i for i, (s, m, _) in enumerate(steps) if s == 'vkvideo'}
    for _i, (_s, _m, _) in enumerate(steps):
        if _s == 'vk' and _m == 'clip_wall':
            if not any(vi < _i for vi in _vkvideo_positions):
                _msg = ('vk.clip_wall зависит от таргета vkvideo, '
                        'но vkvideo отсутствует или стоит позже в списке таргетов — '
                        'исправьте порядок таргетов в настройках')
                write_log_entry(batch_id, category, _msg, level='error')
                write_log_entry(batch_id, category, fmt_id_msg("[publish] Батч {}: {}", batch_id, _msg), level='silent')
                raise AppException(batch_id, 'publish', _msg)

    target_names = ', '.join(t['name'] for t in active_targets)

    if parsed is not None:
        cur_slug, cur_method, cur_phase = parsed
        log_label = f'Публикация (возобновление {cur_slug}.{cur_method}).'
    else:
        log_label = f'Публикация ({target_names}).'

    resume_from = None
    if parsed is not None:
        cur_slug, cur_method, cur_phase = parsed
        if cur_phase == 'pending':
            step_found = any(slug == cur_slug and method == cur_method for slug, method, _ in steps)
            if step_found:
                resume_from = (cur_slug, cur_method)
                write_log_entry(batch_id, category, fmt_id_msg("Возобновление публикации с шага {}.{}", cur_slug, cur_method))
                write_log_entry(batch_id, category, fmt_id_msg("[publish] Батч {} resume с шага {}.{}", batch_id, cur_slug, cur_method), level='silent')
            else:
                msg = f'Шаг {cur_slug}.{cur_method} удалён из конфига — батч требует ручной проверки'
                write_log_entry(batch_id, category, f'Шаг {cur_slug}.{cur_method} более не активен в конфиге — перевожу в ошибку', level='error')
                write_log_entry(batch_id, category, fmt_id_msg("[publish] Батч {} шаг {}.{} не найден в текущем конфиге", batch_id, cur_slug, cur_method), level='silent')
                raise AppException(batch_id, 'publish', msg)
        elif cur_phase in ('completed', 'failed'):
            found_idx = None
            for i, (slug, method, _) in enumerate(steps):
                if slug == cur_slug and method == cur_method:
                    found_idx = i
                    break

            if found_idx is None:
                msg = f'Шаг {cur_slug}.{cur_method} удалён из конфига — батч требует ручной проверки'
                write_log_entry(batch_id, category, msg, level='error')
                write_log_entry(batch_id, category, fmt_id_msg("[publish] Батч {} шаг {}.{} не найден в текущем конфиге", batch_id, cur_slug, cur_method), level='silent')
                raise AppException(batch_id, 'publish', msg)
            elif found_idx + 1 < len(steps):
                ns, nm, _ = steps[found_idx + 1]
                resume_from = (ns, nm)
                write_log_entry(batch_id, category, fmt_id_msg("Продолжаю с шага {}.{}", ns, nm))
                write_log_entry(batch_id, category, fmt_id_msg("[publish] Батч {} resume с шага {}.{}", batch_id, ns, nm), level='silent')
            else:
                db_set_batch_status(batch_id, 'completed')
                _mark_movie_published(batch)
                write_log_entry(batch_id, category, 'Опубликовано (возобновление — все шаги завершены)')
                write_log_entry(batch_id, category, fmt_id_msg("[publish] Батч {} опубликован (возобновление)", batch_id), level='silent')
                return

    write_log_entry(
        batch_id, category,
        fmt_id_msg(
            "[publish] Батч {} — phase=run_mode, parsed_status={}, resume_from={}",
            batch_id,
            bool(parsed),
            resume_from,
        ),
        level='silent',
    )

    pub_title = batch.get('title') or ''
    if pub_title:
        write_log_entry(batch_id, category, f'Заголовок публикации (из БД): «{pub_title}»')
        write_log_entry(batch_id, category, f"Заголовок публикации (из БД): {pub_title}", level='silent')

    ensure_playwright_chromium(batch_id, category)
    write_log_entry(batch_id, category, fmt_id_msg("[publish] Батч {} — phase=playwright_checked", batch_id), level='silent')

    any_ok = False
    failed_steps = []
    expected_from = status
    batch_browser_session = (
        PublishBatchBrowserSession(batch_id, category, steps)
        if pw_step_count(steps) >= 2 else None
    )
    try:
        for step_idx, (slug, method, target) in enumerate(steps):
            if resume_from is not None:
                if (slug, method) != resume_from:
                    continue
                resume_from = None

            posting_status = f"{slug}.{method}.posting"
            completed_status = f"{slug}.{method}.completed"
            failed_status = f"{slug}.{method}.failed"
            write_log_entry(
                batch_id, category,
                fmt_id_msg(
                    "[publish] Батч {} — phase=step_prepare, index={}, step={}.{}, expected_from={}, posting_status={}",
                    batch_id, step_idx + 1, slug, method, expected_from, posting_status,
                ),
                level='silent',
            )

            if not db_claim_batch_status(batch_id, expected_from, posting_status):
                write_log_entry(batch_id, category, 'Батч уже захвачен другим процессом — пропуск')
                write_log_entry(batch_id, category, fmt_id_msg("[publish] Батч {} уже захвачен другим процессом для {} — пропуск", batch_id, posting_status), level='silent')
                return

            if not pub_title:
                pub_title = build_publication_title()
                db_set_batch_title(batch_id, pub_title)
                write_log_entry(batch_id, category, f'Заголовок публикации: «{pub_title}»')
                write_log_entry(batch_id, category, f"Заголовок публикации (новый): {pub_title}", level='silent')

            write_log_entry(batch_id, category, f'Шаг {slug}.{method}: выполняю.')
            write_log_entry(batch_id, category, fmt_id_msg("[publish] Батч {}: шаг {}.{} — начало", batch_id, slug, method), level='silent')

            pw_session = None
            keep_browser = False
            if batch_browser_session is not None and slug in PW_PUBLISH_SLUGS:
                batch_browser_session.set_step_index(step_idx)
                pw_session = batch_browser_session
                keep_browser = batch_browser_session.keep_browser_after_step()

            step_error = None
            try:
                ok = _call_client(
                    slug, method, batch_id, category, target, pub_title,
                    batch_session=pw_session, keep_browser=keep_browser,
                )
            except (DzenSessionMissing, DzenCsrfExpired, RutubeSessionMissing, RutubeCsrfExpired, VkVideoSessionMissing, VkVideoCsrfExpired) as e:
                ok = False
                step_error = str(e)
                write_log_entry(batch_id, category, f'{slug}.{method}: {e}', level='error')
                write_log_entry(batch_id, category, fmt_id_msg("[publish] Батч {} — phase=step_exception, step={}.{}, type={}, msg={}", batch_id, slug, method, type(e).__name__, step_error), level='silent')
            except Exception as e:
                ok = False
                step_error = str(e)
                write_log_entry(batch_id, category, f'{slug}.{method}: неожиданная ошибка: {e}', level='error')
                write_log_entry(batch_id, category, fmt_id_msg("[publish] Батч {} — phase=step_exception, step={}.{}, type={}, msg={}", batch_id, slug, method, type(e).__name__, step_error), level='silent')

            if not ok:
                failed_steps.append(f"{slug}.{method}")
                err_msg = step_error or 'ошибка публикации'
                write_log_entry(batch_id, category, fmt_id_msg("[publish] Батч {} ошибка {}.{}: {}", batch_id, slug, method, err_msg), level='silent')
                db_set_batch_status(batch_id, failed_status)
                expected_from = failed_status
                write_log_entry(batch_id, category, fmt_id_msg("[publish] Батч {} — phase=step_failed, step={}.{}, next_expected_from={}", batch_id, slug, method, expected_from), level='silent')
                if pw_session is not None and pw_session.is_open:
                    pw_session.close()
                    finalize_publish_batch_browser(batch_id, category)
                    batch_browser_session = None
                continue

            any_ok = True

            db_set_batch_status(batch_id, completed_status)
            write_log_entry(batch_id, category, f'Шаг {slug}.{method}: опубликовано')
            write_log_entry(batch_id, category, fmt_id_msg("[publish] Батч {}: шаг {}.{} — завершено успешно", batch_id, slug, method), level='silent')
            expected_from = completed_status
            write_log_entry(batch_id, category, fmt_id_msg("[publish] Батч {} — phase=step_success, step={}.{}, next_expected_from={}", batch_id, slug, method, expected_from), level='silent')
    finally:
        if batch_browser_session is not None and batch_browser_session.is_open:
            batch_browser_session.close()
            finalize_publish_batch_browser(batch_id, category)

    if any_ok:
        if failed_steps:
            fail_list = ', '.join(failed_steps)
            db_set_batch_status(batch_id, 'partially')
            _mark_movie_published(batch)
            write_log_entry(batch_id, category, f'Частично опубликовано; ошибки: {fail_list}')
            write_log_entry(batch_id, category, fmt_id_msg("[publish] Батч {} частично опубликован (ошибки: {})", batch_id, fail_list), level='silent')
            entries = db_get_batch_log_entries(batch_id)
            err_entries = [
                {"msg": e["message"], "level": e["level"], "ts": e["created_at"]}
                for e in entries if e['level'] in ('warn', 'error')
            ]
            notify_failure(
                fmt_id_msg("publish: батч {} частично опубликован; не удалось: {}", batch_id, fail_list),
                log_entries=err_entries,
                partial=True,
            )
            write_log_entry(batch_id, category, fmt_id_msg("[publish] Батч {} — phase=run_done, status=partially", batch_id), level='silent')
        else:
            db_set_batch_status(batch_id, 'completed')
            _mark_movie_published(batch)
            write_log_entry(batch_id, category, f'Успешно опубликовано ({target_names})')
            write_log_entry(batch_id, category, fmt_id_msg("[publish] Батч {} опубликован", batch_id), level='silent')
            entries = db_get_batch_log_entries(batch_id)
            warn_entries = [
                {"msg": e["message"], "level": e["level"], "ts": e["created_at"]}
                for e in entries if e['level'] in ('warn', 'error')
            ]
            if warn_entries:
                notify_failure(
                    fmt_id_msg("publish: батч {} опубликован, но в процессе были некритичные ошибки", batch_id),
                    log_entries=warn_entries,
                    partial=True,
                )
            write_log_entry(batch_id, category, fmt_id_msg("[publish] Батч {} — phase=run_done, status=completed", batch_id), level='silent')
    else:
        abt_msg = f'Ошибка публикации батча в {target_names}'
        write_log_entry(batch_id, category, f'Ошибка публикации батча в {target_names}', level='error')
        write_log_entry(batch_id, category, fmt_id_msg("[publish] Батч {} ошибка публикации", batch_id), level='silent')
        write_log_entry(batch_id, category, fmt_id_msg("[publish] Батч {} — phase=run_done, status=error", batch_id), level='silent')
        raise AppException(batch_id, 'publish', abt_msg)
