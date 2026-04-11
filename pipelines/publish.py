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

from utils.notify import notify_failure
from db import (
    db_get,
    db_get_active_targets,
    db_get_batch_by_id,
    db_get_batch_video_data,
    db_get_batch_original_video,
    db_get_story_title,
    db_set_batch_movie_probe,
    db_set_batch_published,
    db_set_batch_published_partially,
    db_set_batch_publish_error,
    db_set_batch_cancelled,
    db_set_batch_status,
    db_claim_batch_status,
)
from log import db_log_pipeline, db_log_entry, db_log_update, db_get_log_entries
from pipelines.base import check_cancelled, handle_critical_error
from clients import vk
from clients import dzen as dzen_client
from clients.dzen import DzenCsrfExpired, DzenSessionMissing


def _get_video(batch_id, log_id):
    """Возвращает видеоданные батча (transcoded или original).
    Бросает RuntimeError если оба поля NULL."""
    video_data = db_get_batch_video_data(batch_id)
    if video_data is None:
        if log_id:
            db_log_entry(log_id, 'movies.transcoded_data отсутствует — использую оригинал (movies.raw_data)')
        video_data = db_get_batch_original_video(batch_id)
        if video_data is None:
            msg = 'Ни movies.transcoded_data, ни movies.raw_data не найдены в БД — ошибка логики'
            if log_id:
                db_log_entry(log_id, msg, level='error')
            raise RuntimeError(msg)
    return video_data


def _call_client(slug, method, batch_id, log_id, target):
    """Вызывает нужный клиент для данного slug+method. Возвращает True при успехе."""
    cfg = target.get('config') or {}
    target_id = target.get('id')

    if slug == 'vk':
        if not vk.is_configured():
            if log_id:
                db_log_entry(log_id, 'VK_USER_TOKEN не задан', level='error')
            return False
        video_data = _get_video(batch_id, log_id)
        group_id = int(cfg.get('group_id', 236929597))
        batch = db_get_batch_by_id(batch_id)
        story_id = batch.get('story_id') if batch else None
        title = ''
        if story_id:
            try:
                title = db_get_story_title(story_id) or ''
            except Exception:
                pass
        if not title:
            title = 'Видео'
        if method == 'story':
            if log_id:
                db_log_entry(log_id, 'Публикую историю…')
            return vk.publish_story(video_data, group_id, log_id, title=title) is not None
        elif method == 'wall':
            if log_id:
                db_log_entry(log_id, 'Публикую на стену…')
            return vk.publish_wall(video_data, group_id, log_id, title=title) is not None
        else:
            if log_id:
                db_log_entry(log_id, f'VK: неизвестный метод «{method}» — пропуск', level='warn')
            return False

    elif slug == 'dzen':
        if not dzen_client.is_configured(cfg, target_id):
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
                title = db_get_story_title(story_id) or ''
            except Exception:
                pass
        if not title:
            title = 'Видео'


        return dzen_client.publish(video_data, cfg, title, log_id, batch_id=batch_id, target_id=target_id)

    else:
        if log_id:
            db_log_entry(log_id, f'Платформа «{slug}» не поддерживается', level='warn')
        return False


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


def run(batch_id):
    log_id = None
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
                log_id = db_log_pipeline('publish', 'Публикация (пробный)…',
                                         status='running', batch_id=batch_id)
                db_log_entry(log_id, 'Пробный батч — публикация на платформу не выполняется')
                db_set_batch_movie_probe(batch_id)
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

        if not active_targets:
            if parsed is None:
                msg = 'Батч отменён — нет активных таргетов'
                db_set_batch_cancelled(batch_id)
                db_log_pipeline('publish', msg, status='прервана', batch_id=batch_id)
                print(f"[publish] {msg} (батч {batch_id[:8]}…)")
            else:
                msg = 'Нет активных таргетов — публикация невозможна'
                log_id = db_log_pipeline('publish', msg, status='error', batch_id=batch_id)
                db_set_batch_publish_error(batch_id)
                print(f"[publish] {msg}")
                notify_failure(f"publish: {msg} (батч {batch_id[:8]})")
            return

        steps = _build_steps(active_targets)
        if not steps:
            if parsed is None:
                msg = 'Батч отменён — нет методов публикации в конфиге таргетов'
                db_set_batch_cancelled(batch_id)
                db_log_pipeline('publish', msg, status='прервана', batch_id=batch_id)
                print(f"[publish] {msg} (батч {batch_id[:8]}…)")
            else:
                msg = 'Нет методов публикации в конфиге таргетов'
                log_id = db_log_pipeline('publish', msg, status='error', batch_id=batch_id)
                db_set_batch_publish_error(batch_id)
                print(f"[publish] {msg}")
                notify_failure(f"publish: {msg} (батч {batch_id[:8]})")
            return

        target_names = ', '.join(t['name'] for t in active_targets)

        if parsed is not None:
            cur_slug, cur_method, cur_phase = parsed
            log_label = f'Публикация (возобновление {cur_slug}.{cur_method})…'
        else:
            log_label = f'Публикация ({target_names})…'

        log_id = db_log_pipeline('publish', log_label, status='running', batch_id=batch_id)

        resume_from = None
        if parsed is not None:
            cur_slug, cur_method, cur_phase = parsed
            if cur_phase == 'pending':
                step_found = any(slug == cur_slug and method == cur_method for slug, method, _ in steps)
                if step_found:
                    resume_from = (cur_slug, cur_method)
                    print(f"[publish] Батч {batch_id[:8]}… resume с шага {cur_slug}.{cur_method}")
                else:
                    if log_id:
                        db_log_entry(log_id, f'Шаг {cur_slug}.{cur_method} более не активен в конфиге — перевожу в ошибку', level='error')
                    print(f"[publish] Батч {batch_id[:8]}… шаг {cur_slug}.{cur_method} не найден в текущем конфиге")
                    db_set_batch_publish_error(batch_id)
                    db_log_update(log_id, f'Шаг {cur_slug}.{cur_method} удалён из конфига — батч требует ручной проверки', 'error')
                    notify_failure(f"publish: батч {batch_id[:8]} завис в {cur_slug}.{cur_method}.pending, а шаг удалён из конфига")
                    return
            elif cur_phase in ('published', 'failed'):
                found_idx = None
                for i, (slug, method, _) in enumerate(steps):
                    if slug == cur_slug and method == cur_method:
                        found_idx = i
                        break

                if found_idx is None:
                    if log_id:
                        db_log_entry(log_id, f'Шаг {cur_slug}.{cur_method} удалён из конфига — батч требует ручной проверки', level='error')
                    print(f"[publish] Батч {batch_id[:8]}… шаг {cur_slug}.{cur_method} не найден в текущем конфиге")
                    db_set_batch_publish_error(batch_id)
                    db_log_update(log_id, f'Шаг {cur_slug}.{cur_method} удалён из конфига — батч требует ручной проверки', 'error')
                    notify_failure(f"publish: батч {batch_id[:8]} завис в {cur_slug}.{cur_method}.{cur_phase}, а шаг удалён из конфига")
                    return
                elif found_idx + 1 < len(steps):
                    ns, nm, _ = steps[found_idx + 1]
                    resume_from = (ns, nm)
                    print(f"[publish] Батч {batch_id[:8]}… resume с шага {ns}.{nm}")
                else:
                    saved = db_set_batch_published(batch_id)
                    if saved:
                        db_log_update(log_id, 'Опубликовано (возобновление — все шаги завершены)', 'ok')
                        print(f"[publish] Батч {batch_id[:8]}… опубликован (возобновление)")
                    else:
                        db_set_batch_publish_error(batch_id)
                        db_log_update(log_id, 'Ошибка записи published в БД (возобновление)', 'error')
                        notify_failure(f"publish: опубликовано, но статус не сохранён — батч {batch_id[:8]}")
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
                print(f"[publish] Батч {batch_id[:8]}… уже захвачен другим процессом для {posting_status} — пропуск")
                db_log_update(log_id, f'Захват {posting_status} не удался — пропуск', 'ok')
                return

            if log_id:
                db_log_entry(log_id, f'Шаг {slug}.{method}: выполняю…')

            step_error = None
            try:
                ok = _call_client(slug, method, batch_id, log_id, target)
            except (DzenSessionMissing, DzenCsrfExpired) as e:
                ok = False
                step_error = str(e)
                if log_id:
                    db_log_entry(log_id, f'{slug}.{method}: {e}', level='error')
            except Exception as e:
                ok = False
                step_error = str(e)
                if log_id:
                    db_log_entry(log_id, f'{slug}.{method}: неожиданная ошибка: {e}', level='error')

            if not ok:
                failed_steps.append(f"{slug}.{method}")
                err_msg = step_error or 'ошибка публикации'
                print(f"[publish] Батч {batch_id[:8]}… ошибка {slug}.{method}: {err_msg}")
                notify_failure(f"publish: ошибка {slug}.{method} батча {batch_id[:8]}: {err_msg}")
                step_saved = db_set_batch_status(batch_id, failed_status)
                if not step_saved:
                    db_set_batch_publish_error(batch_id)
                    db_log_update(log_id, f'Шаг {slug}.{method}: ошибка, не удалось сохранить статус — аварийная остановка', 'error')
                    return
                expected_from = failed_status
                continue

            any_ok = True

            step_saved = db_set_batch_status(batch_id, published_status)
            if not step_saved:
                db_set_batch_publish_error(batch_id)
                db_log_update(log_id, f'Шаг {slug}.{method}: опубликовано, но ошибка записи статуса в БД', 'error')
                notify_failure(f"publish: шаг {slug}.{method} батча {batch_id[:8]} выполнен, но статус не сохранён")
                return
            if log_id:
                db_log_entry(log_id, f'Шаг {slug}.{method}: опубликовано')
            expected_from = published_status

        if any_ok:
            if failed_steps:
                fail_list = ', '.join(failed_steps)
                saved = db_set_batch_published_partially(batch_id)
                if saved:
                    db_log_update(log_id, f'Частично опубликовано ({target_names}); ошибки: {fail_list}', 'ok')
                    print(f"[publish] Батч {batch_id[:8]}… частично опубликован (ошибки: {fail_list})")
                    entries = db_get_log_entries(log_id) if log_id else []
                    err_entries = [e for e in entries if e['level'] in ('warn', 'error')]
                    notify_failure(
                        f"publish: батч {batch_id[:8]} частично опубликован; не удалось: {fail_list}",
                        log_entries=err_entries,
                        partial=True,
                    )
                else:
                    db_set_batch_publish_error(batch_id)
                    db_log_update(log_id, 'Частично опубликовано, но ошибка записи статуса в БД', 'error')
                    print(f"[publish] Батч {batch_id[:8]}… ошибка записи published_partially в БД")
                    notify_failure(f"publish: частично опубликовано, но статус не сохранён в БД — батч {batch_id[:8]}")
            else:
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
                    db_log_update(log_id, 'Опубликовано, но ошибка записи статуса в БД', 'error')
                    print(f"[publish] Батч {batch_id[:8]}… ошибка записи published в БД")
                    notify_failure(f"publish: опубликовано, но статус не сохранён в БД — батч {batch_id[:8]}")
        else:
            db_set_batch_publish_error(batch_id)
            db_log_update(log_id, 'Ошибка публикации', 'error')
            print(f"[publish] Батч {batch_id[:8]}… ошибка публикации")
            notify_failure(f"publish: ошибка публикации батча {batch_id[:8]} ({target_names})")

    except Exception as e:
        handle_critical_error('publish', batch_id, log_id, e)
