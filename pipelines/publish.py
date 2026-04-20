"""
Pipeline 5 — Публикация.
Принимает batch_id, публикует видео на таргеты по универсальному пайплайну.

Статусы: transcode_ready → {slug}.{method}.posting → {slug}.{method}.published → ... → published
  {slug}.{method}.posting  — активный захват; при рестарте → {slug}.{method}.pending
  {slug}.{method}.pending  — ожидание resume после краша на данном шаге
  {slug}.{method}.published — шаг завершён, следующий шаг не начат

Батчи с *.pending и *.published подхватываются главным циклом через db_get_actionable_batches.
"""

from datetime import datetime, timezone

import common.environment as environment
from utils.notify import notify_failure
from db import (
    db_get,
    db_get_active_targets,
    db_get_batch_by_id,
    db_get_batch_video_data,
    db_get_batch_original_video,
    db_get_story_title,
    db_set_batch_status,
    db_claim_batch_status,
)
from log import db_log_update, db_get_log_entries, write_log_entry
from pipelines.base import check_cancelled
from common.exceptions import AppException
from utils.utils import fmt_id_msg
from clients import vk
from clients import dzen as dzen_client
from clients.dzen import DzenCsrfExpired, DzenSessionMissing


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


def _resolve_title(batch_id, log_id):
    """Возвращает заголовок сюжета батча или 'Видео'."""
    batch = db_get_batch_by_id(batch_id)
    story_id = batch.get('story_id') if batch else None
    if story_id:
        try:
            title = db_get_story_title(story_id) or ''
            if title:
                return title
        except Exception as e:
            write_log_entry(log_id, f"Не удалось получить заголовок сюжета: {e}", level='warn')
    return 'Видео'


def _call_vk(slug, method, batch_id, log_id, target):
    cfg = target.get('config') or {}
    if not vk.is_configured():
        write_log_entry(log_id, 'VK_USER_TOKEN не задан', level='error')
        return False
    video_data = _get_video(batch_id, log_id)
    group_id = int(cfg.get('group_id', 236929597))
    title = _resolve_title(batch_id, log_id)
    if method == 'story':
        write_log_entry(log_id, 'Публикую историю…')
        return vk.publish_story(video_data, group_id, log_id, title=title) is not None
    elif method == 'wall':
        write_log_entry(log_id, 'Публикую на стену…')
        return vk.publish_wall(video_data, group_id, log_id, title=title) is not None
    else:
        write_log_entry(log_id, f'VK: неизвестный метод «{method}» — пропуск', level='warn')
        return False


def _call_dzen(slug, method, batch_id, log_id, target):
    cfg = target.get('config') or {}
    target_id = target.get('id')
    if not dzen_client.is_configured(cfg, target_id):
        if not cfg.get('publisher_id'):
            write_log_entry(log_id, 'Дзен не настроен: publisher_id отсутствует', level='error')
        else:
            write_log_entry(log_id, 'Дзен: браузерная сессия не сохранена — авторизуйтесь на вкладке «Публикация»', level='error')
        return False

    video_data = _get_video(batch_id, log_id)

    title = _resolve_title(batch_id, log_id)
    return dzen_client.publish(video_data, cfg, title, log_id, batch_id=batch_id, target_id=target_id)


_CLIENTS = {
    'vk':   _call_vk,
    'dzen': _call_dzen,
}


def _call_client(slug, method, batch_id, log_id, target):
    """Диспетчеризует вызов клиента по реестру _CLIENTS. Возвращает True при успехе."""
    handler = _CLIENTS.get(slug)
    if handler is None:
        write_log_entry(log_id, f'Платформа «{slug}» не поддерживается', level='warn')
        return False
    return handler(slug, method, batch_id, log_id, target)


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
    try:
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
                db_log_update(log_id, 'Публикация (пробный)…', 'running')
                write_log_entry(log_id, 'Пробный батч — публикация на платформу не выполняется')
                db_set_batch_status(batch_id, 'movie_probe')
                db_log_update(log_id, 'Без публикации (пробный батч)', 'ok')
                write_log_entry(log_id, fmt_id_msg("[publish] Батч {} пробный — публикация пропущена", batch_id))
                return

            if check_cancelled('publish', batch_id, batch, log_id):
                return

            now = datetime.now(timezone.utc)
            if batch['scheduled_at'] is not None and now < batch['scheduled_at']:
                remaining = int((batch['scheduled_at'] - now).total_seconds() / 60)
                write_log_entry(log_id, fmt_id_msg("[publish] Батч {} ещё не время ({} мин до публикации)", batch_id, remaining))
                return

        if not active_targets:
            if parsed is None:
                msg = 'Батч отменён — нет активных таргетов'
                db_set_batch_status(batch_id, 'cancelled')
                db_log_update(log_id, msg, 'cancelled')
                write_log_entry(log_id, fmt_id_msg("[publish] {} (батч {})", msg, batch_id))
                return
            else:
                msg = 'Нет активных таргетов — публикация невозможна'
                db_log_update(log_id, msg, 'error')
                write_log_entry(log_id, f"[publish] {msg}")
                raise AppException(batch_id, 'publish', msg, log_id)

        steps = _build_steps(active_targets)
        if not steps:
            if parsed is None:
                msg = 'Батч отменён — нет методов публикации в конфиге таргетов'
                db_set_batch_status(batch_id, 'cancelled')
                db_log_update(log_id, msg, 'cancelled')
                write_log_entry(log_id, fmt_id_msg("[publish] {} (батч {})", msg, batch_id))
                return
            else:
                msg = 'Нет методов публикации в конфиге таргетов'
                db_log_update(log_id, msg, 'error')
                write_log_entry(log_id, f"[publish] {msg}")
                raise AppException(batch_id, 'publish', msg, log_id)

        target_names = ', '.join(t['name'] for t in active_targets)

        if parsed is not None:
            cur_slug, cur_method, cur_phase = parsed
            log_label = f'Публикация (возобновление {cur_slug}.{cur_method})…'
        else:
            log_label = f'Публикация ({target_names})…'

        db_log_update(log_id, log_label, 'running')

        resume_from = None
        if parsed is not None:
            cur_slug, cur_method, cur_phase = parsed
            if cur_phase == 'pending':
                step_found = any(slug == cur_slug and method == cur_method for slug, method, _ in steps)
                if step_found:
                    resume_from = (cur_slug, cur_method)
                    write_log_entry(log_id, fmt_id_msg("[publish] Батч {} resume с шага {}.{}", batch_id, cur_slug, cur_method))
                else:
                    msg = f'Шаг {cur_slug}.{cur_method} удалён из конфига — батч требует ручной проверки'
                    write_log_entry(log_id, f'Шаг {cur_slug}.{cur_method} более не активен в конфиге — перевожу в ошибку', level='error')
                    write_log_entry(log_id, fmt_id_msg("[publish] Батч {} шаг {}.{} не найден в текущем конфиге", batch_id, cur_slug, cur_method))
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
                    write_log_entry(log_id, fmt_id_msg("[publish] Батч {} шаг {}.{} не найден в текущем конфиге", batch_id, cur_slug, cur_method))
                    db_log_update(log_id, msg, 'error')
                    raise AppException(batch_id, 'publish', msg, log_id)
                elif found_idx + 1 < len(steps):
                    ns, nm, _ = steps[found_idx + 1]
                    resume_from = (ns, nm)
                    write_log_entry(log_id, fmt_id_msg("[publish] Батч {} resume с шага {}.{}", batch_id, ns, nm))
                else:
                    db_set_batch_status(batch_id, 'published')
                    db_log_update(log_id, 'Опубликовано (возобновление — все шаги завершены)', 'ok')
                    write_log_entry(log_id, fmt_id_msg("[publish] Батч {} опубликован (возобновление)", batch_id))
                    return

        any_ok = False
        failed_steps = []
        expected_from = status
        for step_idx, (slug, method, target) in enumerate(steps):
            if resume_from is not None:
                if (slug, method) != resume_from:
                    continue
                resume_from = None

            posting_status = f"{slug}.{method}.posting"
            published_status = f"{slug}.{method}.published"
            failed_status = f"{slug}.{method}.failed"

            if not db_claim_batch_status(batch_id, expected_from, posting_status):
                write_log_entry(log_id, fmt_id_msg("[publish] Батч {} уже захвачен другим процессом для {} — пропуск", batch_id, posting_status))
                db_log_update(log_id, f'Захват {posting_status} не удался — пропуск', 'ok')
                return

            write_log_entry(log_id, f'Шаг {slug}.{method}: выполняю…')

            step_error = None
            try:
                ok = _call_client(slug, method, batch_id, log_id, target)
            except (DzenSessionMissing, DzenCsrfExpired) as e:
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
                write_log_entry(log_id, fmt_id_msg("[publish] Батч {} ошибка {}.{}: {}", batch_id, slug, method, err_msg))
                db_set_batch_status(batch_id, failed_status)
                expected_from = failed_status
                continue

            any_ok = True

            db_set_batch_status(batch_id, published_status)
            write_log_entry(log_id, f'Шаг {slug}.{method}: опубликовано')
            expected_from = published_status

        if any_ok:
            if failed_steps:
                fail_list = ', '.join(failed_steps)
                db_set_batch_status(batch_id, 'published_partially')
                db_log_update(log_id, f'Частично опубликовано ({target_names}); ошибки: {fail_list}', 'ok')
                write_log_entry(log_id, fmt_id_msg("[publish] Батч {} частично опубликован (ошибки: {})", batch_id, fail_list))
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
                write_log_entry(log_id, fmt_id_msg("[publish] Батч {} опубликован", batch_id))
                if log_id:
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
            write_log_entry(log_id, fmt_id_msg("[publish] Батч {} ошибка публикации", batch_id))
            raise AppException(batch_id, 'publish', abt_msg, log_id)

    except AppException:
        raise
