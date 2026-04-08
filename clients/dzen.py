"""
Дзен-клиент: публикует короткое видео через веб-интерфейс Дзена (UI-driven).

Playwright управляет браузером, скриншоты стримятся в виджет «Публикация».
Видео загружается через set_input_files() — как обычный пользователь.
"""

import os
import re
import shutil
import tempfile
import time as _time

from log import db_log_entry


_NAV_TIMEOUT = 30_000   # ms — таймаут навигации
_UPLOAD_WAIT  = 60_000  # ms — ожидание завершения загрузки видео


class DzenSessionMissing(RuntimeError):
    """Браузерная сессия Дзен не сохранена — требуется авторизация."""


class DzenCsrfExpired(RuntimeError):
    """Сессия истекла — необходима повторная авторизация."""


class DzenApiError(RuntimeError):
    """Ошибка публикации на Дзен."""


# ---------------------------------------------------------------------------
# Проверка конфигурации
# ---------------------------------------------------------------------------

def is_configured(cfg: dict, target_id: str | None = None) -> bool:
    """True если publisher_id задан и сессия сохранена в БД."""
    from services.dzen_browser import profile_exists
    return bool(cfg.get("publisher_id")) and profile_exists(target_id)


# ---------------------------------------------------------------------------
# Публичный API
# ---------------------------------------------------------------------------

def publish(
    video_data: bytes,
    target_config: dict,
    title: str,
    log_id,
    batch_id=None,
    target_id: str | None = None,
) -> bool:
    """
    Публикует видео на Дзен через веб-интерфейс.
    Браузер виден в панели «Публикация» — можно наблюдать весь процесс.
    Возвращает True при успехе.
    """
    from services.dzen_browser import run_pipeline_browser
    from db import db_get_target_session_context

    cfg = target_config or {}
    publisher_id = cfg.get("publisher_id", "")

    if not publisher_id:
        raise DzenApiError("publisher_id не задан в настройках Дзен")

    if not target_id:
        raise DzenSessionMissing("target_id не передан — невозможно загрузить сессию")

    session = db_get_target_session_context(target_id)
    if not session:
        raise DzenSessionMissing(
            "Браузерная сессия Дзен не сохранена — "
            "авторизуйтесь в браузере (вкладка «Публикация»)"
        )

    saved_cookies = session.get("cookies", [])

    if log_id:
        db_log_entry(log_id, f"Дзен: {len(video_data) // 1024} КБ, publisher={publisher_id[:12]}…")

    # Пишем видео во временный файл с именем = заголовок (Дзен автоподставляет имя файла)
    safe_name = re.sub(r'[^\w\s\-]', '', title, flags=re.UNICODE).strip()
    safe_name = re.sub(r'\s+', '_', safe_name)[:80] or "video"
    tmp_dir = tempfile.mkdtemp()
    video_path = os.path.join(tmp_dir, f"{safe_name}.mp4")
    try:
        with open(video_path, "wb") as _f:
            _f.write(video_data)

        def _do_publish(page, _ctx):
            _publish_ui(page, publisher_id, video_path, title, log_id, batch_id=batch_id)

        result = run_pipeline_browser(_do_publish, saved_cookies)

        if not result["ok"]:
            err = result.get("error", "Неизвестная ошибка")
            if "истекла" in err or "авторизуйтесь" in err:
                raise DzenCsrfExpired(err)
            raise DzenApiError(err)

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    if log_id:
        db_log_entry(log_id, "Дзен: видео опубликовано успешно")
    return True


# ---------------------------------------------------------------------------
# UI-driven публикация
# ---------------------------------------------------------------------------

def _log(log_id, msg: str):
    print(f"[dzen] {msg}")
    if log_id:
        db_log_entry(log_id, f"Дзен: {msg}")


def _snap(page, batch_id=None) -> None:
    """Снимает скриншот и передаёт кадр в SSE-трансляцию и монитор (thread-safe)."""
    try:
        from services.dzen_browser import push_frame, push_frame_for_batch
        img = page.screenshot(type="jpeg", quality=65)
        push_frame(img)
        if batch_id:
            push_frame_for_batch(batch_id, img)
    except Exception as _e:
        print(f"[dzen] _snap: {_e}")


def _publish_ui(page, publisher_id: str, video_path: str, title: str, log_id, batch_id=None):
    """Управляет браузером для публикации видео через UI Дзена."""

    studio_url = f"https://dzen.ru/profile/editor/id/{publisher_id}/"

    # ── Шаг 1: Переходим в студию ────────────────────────────────────────
    _log(log_id, f"Переход в студию: {studio_url}")
    page.goto(studio_url, wait_until="domcontentloaded", timeout=_NAV_TIMEOUT)
    page.wait_for_timeout(2000)
    _snap(page, batch_id)

    cur = page.url
    print(f"[dzen] URL после перехода: {cur}")
    if "passport.yandex" in cur or "/auth" in cur:
        raise DzenCsrfExpired(
            "Сессия истекла — авторизуйтесь снова в браузере (вкладка «Публикация»)"
        )

    # ── Закрываем модальный overlay если есть (онбординг, донаты и т.п.) ─
    try:
        overlay = page.locator("[data-testid='modal-overlay']").first
        if overlay.is_visible():
            _log(log_id, "Закрываю модальное окно…")
            # Сначала пробуем кнопку ×, затем клик по оверлею
            close_x = page.locator(
                "[data-testid='modal-overlay'] ~ * button, "
                "dialog button[aria-label*='lose'], "
                "dialog button[aria-label*='закр'], "
                "[class*='close'], [class*='Close']"
            ).first
            try:
                if close_x.is_visible():
                    close_x.click()
                else:
                    overlay.click()
            except Exception:
                overlay.click()
            page.wait_for_timeout(500)
            _snap(page, batch_id)
    except Exception:
        pass
    # На всякий случай — Escape
    try:
        page.keyboard.press("Escape")
        page.wait_for_timeout(300)
    except Exception:
        pass

    # ── Шаг 2: Кнопка «+» (плюсик) в правом верхнем углу ────────────────
    _log(log_id, "Ищу кнопку «+» для создания публикации…")
    plus_btn = page.locator(
        "[class*='addButton'], "
        "[class*='author-studio-header__addButton'], "
        "[data-testid='add-publication-button'], "
        "button[aria-label*='Создать'], "
        "button[aria-label*='создать'], "
        "button[title*='Создать'], "
        "button[aria-label*='Create']"
    ).first
    plus_btn.wait_for(state="visible", timeout=15_000)
    plus_btn.click()
    _log(log_id, "Кнопка «+» нажата, жду меню…")
    page.wait_for_timeout(1500)
    _snap(page, batch_id)

    # ── Шаг 3: «Загрузить видео» из выпадающего меню ─────────────────────
    _log(log_id, "Выбираю «Загрузить видео»…")
    upload_item = page.get_by_text("Загрузить видео", exact=True).first
    try:
        upload_item.wait_for(state="visible", timeout=8_000)
    except Exception:
        _log(log_id, "exact-match не нашёл — пробую contains…")
        upload_item = page.locator("text=Загрузить видео").first
        upload_item.wait_for(state="visible", timeout=5_000)
    upload_item.click()
    _log(log_id, "«Загрузить видео» нажато")
    page.wait_for_timeout(1500)
    _snap(page, batch_id)

    # ── Шаг 4: Загружаем файл ────────────────────────────────────────────
    _log(log_id, "Ищу поле загрузки файла…")
    # file input скрыт намеренно — ждём только "attached", не "visible"
    file_input = page.locator('input[type="file"]').first
    file_input.wait_for(state="attached", timeout=15_000)
    file_input.set_input_files(video_path)
    _log(log_id, "Файл передан браузеру, жду загрузки…")
    _snap(page, batch_id)

    # Ждём пока прогресс-бар исчезнет или появится кнопка следующего шага
    try:
        page.wait_for_selector(
            "button:has-text('Опубликовать'), "
            "input[placeholder*='аголов'], "
            "textarea[placeholder*='аголов']",
            timeout=_UPLOAD_WAIT,
        )
    except Exception:
        _log(log_id, "Не дождался явного сигнала — продолжаю…")
        page.wait_for_timeout(5000)
    _snap(page, batch_id)

    # ── Шаг 5: Публикуем ─────────────────────────────────────────────────
    _log(log_id, "Нажимаю «Опубликовать»…")
    pub_btn = page.locator("button:has-text('Опубликовать')").first
    pub_btn.wait_for(state="visible", timeout=15_000)
    pub_btn.click()
    page.wait_for_timeout(2000)
    _snap(page, batch_id)

    # ── Шаг 6: Обрабатываем три разных элемента (25 секунд) ─────────────
    #
    # A. Кнопка «Опубликовать после обработки» — нажать немедленно при появлении.
    # B. Капча VK «Я не робот» (iframe id.vk.com/not_robot_captcha) — кликнуть
    #    ТОЛЬКО чекбокс внутри iframe капчи, не трогать ничего снаружи.
    # C. «Уже можно публиковать» — это просто текст внизу диалога, НЕ попап.
    #    Закрывать нечего, игнорируем. Escape не слать — он убьёт капчу.
    #
    _DIALOG_WINDOW = 25_000  # ms
    _DIALOG_POLL   = 2_000   # ms
    captcha_clicked = False

    _dialog_deadline = _time.monotonic() + _DIALOG_WINDOW / 1000

    while _time.monotonic() < _dialog_deadline:

        # ── A. Кнопка «Опубликовать после обработки» ──────────────────────
        try:
            pub_after = page.locator(
                "button:has-text('Опубликовать после обработки')"
            ).first
            if pub_after.is_visible():
                _log(log_id, "Нажимаю «Опубликовать после обработки»…")
                pub_after.click()
                page.wait_for_timeout(2000)
                _snap(page, batch_id)
        except Exception:
            pass

        # ── B. Капча «Я не робот» — кликаем чекбокс внутри iframe ────────
        if not captcha_clicked:
            try:
                all_frames = page.frames
                _log(log_id, f"Фреймы: {[f.url for f in all_frames]}")
                for frame in all_frames:
                    if captcha_clicked:
                        break
                    furl = frame.url.lower()
                    # Только фреймы капчи (id.vk.com/not_robot_captcha, smartcaptcha, и т.п.)
                    is_captcha = any(kw in furl for kw in (
                        "not_robot_captcha", "smartcaptcha", "yandexcloud",
                        "captcha.yandex", "recaptcha", "id.vk.com",
                    ))
                    if not is_captcha:
                        continue
                    _log(log_id, f"Капча-фрейм найден: {frame.url!r}")
                    # Ищем чекбокс ВНУТРИ фрейма капчи
                    for sel in [
                        "[class*='vkc__Checkbox']",
                        "input[type='checkbox']",
                        "[role='checkbox']",
                        "label",
                    ]:
                        try:
                            el = frame.locator(sel).first
                            if el.is_visible():
                                _log(log_id, f"Кликаю чекбокс капчи {sel!r}…")
                                try:
                                    el.click(timeout=2000)
                                except Exception:
                                    el.click(force=True, timeout=2000)
                                page.wait_for_timeout(2000)
                                captcha_clicked = True
                                _snap(page, batch_id)
                                break
                        except Exception:
                            pass
            except Exception:
                pass

        # ── C. «Уже можно публиковать» — это встроенный текст диалога, НЕ попап.
        #       Закрывать нечего, игнорируем полностью. Escape не отправляем.

        # ── Проверяем финальное подтверждение публикации ──────────────────
        try:
            success_now = page.locator(
                "[class*='toast']:has-text('опубликован'), "
                "[class*='notification']:has-text('опубликован'), "
                "[data-testid='publish-success']"
            ).first
            if success_now.is_visible():
                _log(log_id, "Публикация подтверждена ещё в шаге 6.")
                captcha_clicked = True
                break
        except Exception:
            pass

        page.wait_for_timeout(_DIALOG_POLL)

    if captcha_clicked:
        _log(log_id, "Действия в шаге 6 выполнены, жду подтверждения публикации…")
    else:
        _log(log_id, "Шаг 6 завершён (капча/попап не обнаружены), жду подтверждения…")

    # ── Шаг 7: Ожидаем подтверждения публикации ──────────────────────────
    _PUBLISH_CONFIRM_TIMEOUT = 60_000  # ms — полный таймаут ожидания
    _CONFIRM_POLL = 2_000              # ms — интервал опроса

    url_before = page.url
    confirmed = False

    # CSS-селекторы (только чистый CSS, без text= — они несовместимы с wait_for_selector)
    css_success_selector = (
        "[class*='toast']:has-text('опубликован'), "
        "[class*='notification']:has-text('опубликован'), "
        "[data-testid='publish-success'], "
        "[data-testid*='publish']:has-text('опубликован')"
    )
    # Текстовые паттерны — проверяем отдельно через locator.
    # ВАЖНО: «Уже можно публиковать» — это подсказка ДО публикации, не подтверждение.
    # Сюда включаем только то, что появляется ПОСЛЕ успешной отправки.
    text_success_patterns = [
        "text=Видео опубликовано",
        "text=Видео добавлено",
        "text=Видео будет опубликовано",
        "text=Видео на модерации",
        "text=Видео обрабатывается",
        "text=Ролик опубликован",
    ]

    # Снимок на каждой навигации главного фрейма (смена URL, редирект и т.п.)
    def _on_navigate(frame):
        if frame == page.main_frame:
            _snap(page, batch_id)
    page.on("framenavigated", _on_navigate)

    _confirm_deadline = _time.monotonic() + _PUBLISH_CONFIRM_TIMEOUT / 1000
    _snap_every = 3   # опросный снимок каждые N итераций (каждые 6 сек при POLL=2s)
    _iter = 0
    while _time.monotonic() < _confirm_deadline and not confirmed:
        _iter += 1
        if _iter % _snap_every == 1:   # первый снимок сразу, потом каждые 6 сек
            _snap(page, batch_id)

        # 1. CSS-проверка
        try:
            el = page.locator(css_success_selector).first
            if el.is_visible():
                confirmed = True
                _log(log_id, "Уведомление об успешной публикации получено (CSS).")
                break
        except Exception:
            pass

        # 2. Текстовая проверка
        for pat in text_success_patterns:
            try:
                el = page.locator(pat).first
                if el.is_visible():
                    confirmed = True
                    _log(log_id, f"Успех по тексту: {pat!r}")
                    break
            except Exception:
                pass
        if confirmed:
            break

        # 3. Проверка URL
        url_now = page.url
        if url_now != url_before:
            video_match = re.search(r"/video/|/shorts/|/watch\?", url_now)
            if video_match or "editor" not in url_now:
                confirmed = True
                _log(log_id, f"URL сменился ({url_now}) — публикация подтверждена.")
                break

        page.wait_for_timeout(_CONFIRM_POLL)

    page.remove_listener("framenavigated", _on_navigate)

    if not confirmed:
        # Финальный URL-снимок
        url_after = page.url
        print(f"[dzen] URL до публикации: {url_before}")
        print(f"[dzen] URL после публикации: {url_after}")
        video_url_pattern = re.search(r"/video/|/shorts/|/watch\?", url_after)
        if video_url_pattern and url_after != url_before:
            confirmed = True
            _log(log_id, f"URL сменился на страницу видео ({url_after}) — публикация подтверждена.")
        elif url_after != url_before and "editor" not in url_after:
            confirmed = True
            _log(log_id, f"URL сменился ({url_after}) — публикация предположительно подтверждена.")

    _snap(page, batch_id)

    if not confirmed:
        raise DzenApiError(
            "Публикация не подтверждена в течение 60 секунд. "
            "Возможно, осталась необработанная капча или произошла ошибка на стороне Дзена."
        )

    final_url = page.url
    print(f"[dzen] URL после публикации: {final_url}")
    _log(log_id, "Публикация завершена!")
