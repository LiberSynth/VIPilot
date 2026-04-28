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

from log import write_log_entry
from utils.utils import fmt_id_msg
from routes.api import build_publication_title, publication_file_name, tags


_NAV_TIMEOUT = 30_000   # ms — таймаут навигации
_UPLOAD_WAIT  = 60_000  # ms — ожидание завершения загрузки видео


class DzenSessionMissing(RuntimeError):
    """Браузерная сессия Дзен не сохранена — требуется авторизация."""


class DzenCsrfExpired(RuntimeError):
    """Сессия истекла — необходима повторная авторизация."""


class DzenApiError(RuntimeError):
    """Ошибка публикации на Дзен."""


# ---------------------------------------------------------------------------
# Публичный API
# ---------------------------------------------------------------------------

def publish(
    video_data: bytes,
    target_config: dict,
    log_id,
    batch_id=None,
    target_id: str | None = None,
) -> bool:
    """
    Публикует видео на Дзен через веб-интерфейс.
    Браузер виден в панели «Публикация» — можно наблюдать весь процесс.
    Возвращает True при успехе.
    """
    from services.browser_registry import get_browser as _get_browser
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

    write_log_entry(log_id, "Дзен: Публикация запущена.")
    write_log_entry(log_id, fmt_id_msg("[dzen] {} КБ, publisher={}", len(video_data) // 1024, publisher_id), level='silent')

    # Пишем видео во временный файл с именем = заголовок (Дзен автоподставляет имя файла)
    pub_title = build_publication_title()
    file_name = publication_file_name(pub_title)
    write_log_entry(log_id, f"[dzen] Заголовок: {pub_title}, файл: {file_name}", level='silent')
    tmp_dir = tempfile.mkdtemp()
    video_path = os.path.join(tmp_dir, file_name)
    try:
        with open(video_path, "wb") as _f:
            _f.write(video_data)

        def _do_publish(page, _ctx):
            _publish_ui(page, publisher_id, video_path, log_id, batch_id=batch_id)

        result = _get_browser("dzen").run_pipeline_browser(_do_publish, saved_cookies)

        if not result["ok"]:
            err = result.get("error", "Неизвестная ошибка")
            if "истекла" in err or "авторизуйтесь" in err:
                raise DzenCsrfExpired(err)
            raise DzenApiError(err)

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    write_log_entry(log_id, "Дзен: видео опубликовано успешно")
    return True


# ---------------------------------------------------------------------------
# UI-driven публикация
# ---------------------------------------------------------------------------

def _snap(page, batch_id=None) -> None:
    """Снимает скриншот и передаёт кадр в SSE-трансляцию и монитор (thread-safe)."""
    try:
        from services.browser_registry import get_browser as _get_browser
        _b = _get_browser("dzen")
        img = page.screenshot(type="jpeg", quality=65)
        _b.push_frame(img)
        if batch_id:
            _b.push_frame_for_batch(batch_id, img)
    except Exception as _e:
        write_log_entry(None, f"[dzen] _snap: {_e}", level='silent')


def _has_captcha_frame(page) -> bool:
    """Возвращает True если в page есть активный iframe капчи."""
    try:
        for frame in page.frames:
            furl = frame.url.lower()
            if any(kw in furl for kw in (
                "not_robot_captcha", "smartcaptcha", "yandexcloud",
                "captcha.yandex", "recaptcha", "id.vk.com",
            )):
                return True
    except Exception:
        pass
    return False


def _has_publish_confirm_dialog(page) -> bool:
    """Возвращает True если в DOM видна кнопка подтверждения публикации."""
    for text in ("Опубликовать после обработки", "Опубликовать после подтверждения"):
        try:
            btn = page.locator(f"button:has-text('{text}')")
            if btn.count() > 0 and btn.first.is_visible(timeout=300):
                return True
        except Exception:
            pass
    return False


def _dismiss_popups(page, log_id=None) -> None:
    """
    Закрывает любые видимые диалоги/попапы без разбора.
    Исключения (не трогаем):
      1. Диалог загрузки файла — input[type=file] в DOM сигнализирует,
         что Дзен показывает страницу/модалку загрузки видео; Escape
         закроет её до того, как файл будет передан браузеру.
      2. Активная капча-iframe — иначе сломаем прохождение капчи.
      3. Диалог подтверждения публикации — кнопка «Опубликовать после обработки»
         или «Опубликовать после подтверждения» видна в DOM.
    """
    try:
        if page.locator('input[type="file"]').count() > 0:
            return
    except Exception:
        pass
    if _has_captcha_frame(page):
        return
    if _has_publish_confirm_dialog(page):
        return
    try:
        page.keyboard.press("Escape")
        page.wait_for_timeout(200)
    except Exception:
        pass
    for sel in [
        "[data-testid='modal-overlay']",
        "[class*='modal-close']",
        "[class*='modalClose']",
        "button[aria-label*='lose']",
        "button[aria-label*='закр']",
        "dialog button",
        "[class*='close'][class*='button']",
    ]:
        try:
            btn = page.locator(sel).first
            if btn.is_visible(timeout=300):
                btn.click()
                page.wait_for_timeout(200)
                break
        except Exception:
            pass


def _publish_ui(page, publisher_id: str, video_path: str, log_id, batch_id=None):
    """Управляет браузером для публикации видео через UI Дзена."""

    studio_url = f"https://dzen.ru/profile/editor/id/{publisher_id}/"

    # ── Шаг 1: Переходим в студию ────────────────────────────────────────
    write_log_entry(log_id, "Дзен: Переход в студию.")
    write_log_entry(log_id, f"[dzen] URL студии: {studio_url}", level='silent')
    page.goto(studio_url, wait_until="domcontentloaded", timeout=_NAV_TIMEOUT)
    page.wait_for_timeout(2000)
    _snap(page, batch_id)

    cur = page.url
    write_log_entry(log_id, f"[dzen] URL после перехода: {cur}", level='silent')
    if "passport.yandex" in cur or "/auth" in cur:
        raise DzenCsrfExpired(
            "Сессия истекла — авторизуйтесь снова в браузере (вкладка «Публикация»)"
        )

    # ── Закрываем модальный overlay если есть (онбординг, донаты и т.п.) ─
    try:
        overlay = page.locator("[data-testid='modal-overlay']").first
        if overlay.is_visible():
            write_log_entry(log_id, "Дзен: Закрываю модальное окно.")
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

    # Перед шагом 2: сбрасываем любые оставшиеся диалоги
    _dismiss_popups(page, log_id)

    # ── Шаг 2: Кнопка «+» (плюсик) в правом верхнем углу ────────────────
    write_log_entry(log_id, "Дзен: Ищу кнопку «+» для создания публикации.")
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
    write_log_entry(log_id, "Дзен: Кнопка «+» нажата, жду меню.")
    page.wait_for_timeout(1500)
    _snap(page, batch_id)

    # ── Шаг 3: «Загрузить видео» из выпадающего меню ─────────────────────
    write_log_entry(log_id, "Дзен: Выбираю «Загрузить видео».")
    upload_item = page.get_by_text("Загрузить видео", exact=True).first
    try:
        upload_item.wait_for(state="visible", timeout=8_000)
    except Exception:
        write_log_entry(log_id, "Дзен: exact-match не нашёл — пробую contains.")
        upload_item = page.locator("text=Загрузить видео").first
        upload_item.wait_for(state="visible", timeout=5_000)
    upload_item.click()
    write_log_entry(log_id, "Дзен: «Загрузить видео» нажато")
    page.wait_for_timeout(1500)
    _snap(page, batch_id)

    # ── Шаг 4: Загружаем файл ────────────────────────────────────────────
    write_log_entry(log_id, "Дзен: Ищу поле загрузки файла.")
    # Ждём появления кнопки ДО входа в expect_file_chooser:
    # если войти до того, как кнопка видна, click() зависает внутри with-блока
    # и expect_file_chooser истекает раньше, чем диалог успевает открыться.
    choose_btn = page.get_by_text("Выбрать видео", exact=False).first
    choose_btn.wait_for(state="visible", timeout=20_000)
    write_log_entry(log_id, "Дзен: Кнопка «Выбрать видео» найдена, открываю диалог выбора файла.")
    with page.expect_file_chooser(timeout=15_000) as fc_info:
        choose_btn.click()
    file_chooser = fc_info.value
    file_chooser.set_files(video_path)
    write_log_entry(log_id, "Дзен: Файл передан браузеру, жду загрузки.")
    write_log_entry(log_id, f"[dzen] Файл: {os.path.basename(video_path)}", level='silent')
    _snap(page, batch_id)

    # Ждём одно из двух:
    #   a) ?videoEditorPublicationId=...  — редактор открылся, нужно кликать «Опубликовать»
    #   b) ?state=published               — Дзен опубликовал сам, ничего больше не нужно
    write_log_entry(log_id, "Дзен: Жду открытия редактора видео или авто-публикации.")
    _editor_opened = False
    _auto_published = False
    try:
        page.wait_for_url(
            re.compile(r"videoEditorPublicationId|state=published"),
            timeout=_UPLOAD_WAIT,
        )
        _cur = page.url
        if "state=published" in _cur:
            _auto_published = True
            write_log_entry(log_id, "Дзен: Видео опубликовано автоматически.")
            write_log_entry(log_id, f"[dzen] URL авто-публикации: {_cur}", level='silent')
        else:
            _editor_opened = True
            write_log_entry(log_id, "Дзен: Редактор видео открылся.")
            write_log_entry(log_id, f"[dzen] URL редактора: {_cur}", level='silent')
    except Exception:
        pass

    if _auto_published:
        # Видео уже опубликовано — пропускаем шаги 5-7
        _snap(page, batch_id)
        write_log_entry(log_id, "Дзен: Публикация завершена.")
        return

    if not _editor_opened:
        # Запасной вариант: ждём поле заголовка или кнопку в диалоге
        write_log_entry(log_id, "Дзен: URL редактора не появился, жду форму.")
        try:
            page.wait_for_selector(
                "input[placeholder*='аголов'], "
                "textarea[placeholder*='аголов'], "
                "button:has-text('Опубликовать после обработки')",
                timeout=15_000,
            )
        except Exception:
            write_log_entry(log_id, "Дзен: Форма не обнаружена — продолжаю по таймауту.")
            page.wait_for_timeout(5000)
    _snap(page, batch_id)

    # ── Шаг 5: Заполняем теги ────────────────────────────────────────────
    write_log_entry(log_id, "Дзен: Заполняю теги.")
    write_log_entry(log_id, f"[dzen] Теги: {tags()}", level='silent')
    try:
        tags_input = page.locator(
            "input[placeholder*='теги'], "
            "input[placeholder*='Теги']"
        ).first
        tags_input.wait_for(state="visible", timeout=5_000)
        tags_input.click()
        for tag in tags():
            tags_input.type(tag)
            page.keyboard.press("Enter")
            page.wait_for_timeout(300)
        write_log_entry(log_id, "Дзен: Теги заполнены")
        _snap(page, batch_id)
    except Exception as _e:
        write_log_entry(log_id, "Дзен: Не удалось заполнить теги — продолжаю.")
        write_log_entry(log_id, f"[dzen] Ошибка тегов: {_e}", level='silent')

    # ── Шаг 6: Публикуем ─────────────────────────────────────────────────
    write_log_entry(log_id, "Дзен: Нажимаю «Опубликовать».")
    pub_btn = page.locator("button:has-text('Опубликовать')").first
    pub_btn.wait_for(state="visible", timeout=15_000)
    pub_btn.click()
    page.wait_for_timeout(2000)
    _snap(page, batch_id)

    # ── Шаг 7: Обрабатываем три разных элемента (25 секунд) ─────────────
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

    # Тексты, которые Дзен показывает в тост-ошибках при неудаче публикации.
    _DZEN_ERROR_TEXTS = [
        "временно ограничена",
        "Публикация материалов",
        "обратитесь в поддержку",
        "Ошибка публикации",
        "не удалось опубликовать",
        "Видео не опубликовано",
    ]

    def _check_error_toast():
        """Проверяет body на наличие известных ошибок Дзена; кидает DzenApiError."""
        try:
            body = page.locator("body").inner_text(timeout=1500)
            for err in _DZEN_ERROR_TEXTS:
                if err.lower() in body.lower():
                    raise DzenApiError(
                        f"Дзен заблокировал публикацию: «{err}». "
                        "Попробуйте позже или проверьте состояние аккаунта."
                    )
        except DzenApiError:
            raise
        except Exception:
            pass

    _dialog_deadline = _time.monotonic() + _DIALOG_WINDOW / 1000

    while _time.monotonic() < _dialog_deadline:

        # ── A. Кнопки подтверждения публикации ────────────────────────────
        # Дзен показывает одну из двух кнопок после нажатия «Опубликовать»:
        #   • «Опубликовать после подтверждения» — видео ещё обрабатывается
        #   • «Опубликовать» — видео готово (может совпадать с текстом формы,
        #                       но форма к этому моменту уже скрыта)
        # «Опубликовать после обработки» — старый вариант текста, оставляем на всякий случай.
        try:
            pub_after = page.locator(
                "button:has-text('Опубликовать после подтверждения'), "
                "button:has-text('Опубликовать после обработки'), "
                "button:has-text('Опубликовать')"
            ).first
            if pub_after.is_visible(timeout=300):
                btn_text = pub_after.inner_text()
                write_log_entry(log_id, "Дзен: Нажимаю кнопку подтверждения публикации.")
                write_log_entry(log_id, f"[dzen] Текст кнопки: «{btn_text}»", level='silent')
                pub_after.click()
                page.wait_for_timeout(2000)
                _snap(page, batch_id)
                _check_error_toast()
        except DzenApiError:
            raise
        except Exception:
            pass

        # ── B. Капча «Я не робот» — кликаем чекбокс внутри iframe ────────
        if not captcha_clicked:
            try:
                all_frames = page.frames
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
                    write_log_entry(log_id, "Дзен: Капча-фрейм найден")
                    write_log_entry(log_id, f"[dzen] URL капча-фрейма: {furl}", level='silent')
                    # Вариант 1: JS-клик напрямую по input[type=checkbox] внутри фрейма.
                    # Это надёжнее чем Playwright-клик — не зависит от видимости и позиции.
                    _js_clicked = False
                    for js_sel in [
                        'input[type="checkbox"]',
                        '[class*="Checkbox"] input',
                        '[class*="checkbox"] input',
                    ]:
                        try:
                            done = frame.evaluate(
                                f'() => {{ const el = document.querySelector({repr(js_sel)}); '
                                f'if (el) {{ el.click(); return true; }} return false; }}'
                            )
                            if done:
                                write_log_entry(log_id, f"[dzen] Капча: JS-клик по {js_sel!r} — выполнен", level='silent')
                                _js_clicked = True
                                break
                        except Exception:
                            pass

                    if _js_clicked:
                        page.wait_for_timeout(2000)
                        write_log_entry(log_id, "Дзен: Капча-фрейм пройден.")
                        write_log_entry(log_id, "[dzen] Капча пройдена через JS-клик.", level='silent')
                        captcha_clicked = True
                        _snap(page, batch_id)
                        break

                    # Вариант 2: Playwright-клик по label (ассоциирован с чекбоксом через for=)
                    for sel in [
                        "label",
                        "input[type='checkbox']",
                        "[role='checkbox']",
                    ]:
                        try:
                            el = frame.locator(sel).first
                            if el.is_visible():
                                write_log_entry(log_id, f"[dzen] Капча: Playwright-клик {sel!r}", level='silent')
                                el.click(force=True, timeout=2000)
                                page.wait_for_timeout(2000)
                                write_log_entry(log_id, "Дзен: Капча-фрейм пройден.")
                                write_log_entry(log_id, f"[dzen] Капча пройдена через Playwright-клик {sel!r}.", level='silent')
                                captcha_clicked = True
                                _snap(page, batch_id)
                                break
                        except Exception:
                            pass
                    if captcha_clicked:
                        break
            except Exception:
                pass

        # ── C. «Уже можно публиковать» — это встроенный текст диалога, НЕ попап.
        #       Закрывать нечего, игнорируем полностью. Escape не отправляем.

        # ── D. Закрываем любые другие неизвестные попапы/диалоги ──────────
        #       _dismiss_popups сама охраняет капчу и диалог загрузки файла.
        if not captcha_clicked:
            _dismiss_popups(page, log_id)

        # ── Проверяем финальное подтверждение публикации ──────────────────
        try:
            success_now = page.locator(
                "[class*='toast']:has-text('опубликован'), "
                "[class*='notification']:has-text('опубликован'), "
                "[data-testid='publish-success']"
            ).first
            if success_now.is_visible():
                write_log_entry(log_id, "Дзен: Публикация подтверждена ещё в шаге 6.")
                captcha_clicked = True
                break
        except Exception:
            pass

        page.wait_for_timeout(_DIALOG_POLL)

    if captcha_clicked:
        write_log_entry(log_id, "Дзен: Действия в шаге 7 выполнены, жду подтверждения публикации.")
    else:
        write_log_entry(log_id, "Дзен: Шаг 7 завершён (капча/попап не обнаружены), жду подтверждения.")

    # ── Шаг 8: Ожидаем подтверждения публикации ──────────────────────────
    _PUBLISH_CONFIRM_TIMEOUT = 60_000  # ms — полный таймаут ожидания
    _CONFIRM_POLL = 2_000              # ms — интервал опроса

    url_before = page.url
    confirmed = False

    # Быстрая проверка: браузер уже на странице подтверждения ещё до цикла
    if "state=published" in url_before or "state=pending" in url_before:
        state_label = "state=published" if "state=published" in url_before else "state=pending"
        confirmed = True
        write_log_entry(log_id, f"Дзен: URL → {state_label} — публикация подтверждена.")
        write_log_entry(log_id, f"[dzen] Полный URL: {url_before}", level='silent')

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
                write_log_entry(log_id, "Дзен: Уведомление об успешной публикации получено (CSS).")
                write_log_entry(log_id, f"[dzen] URL: {page.url}", level='silent')
                break
        except Exception:
            pass

        # 2. Текстовая проверка
        for pat in text_success_patterns:
            try:
                el = page.locator(pat).first
                if el.is_visible():
                    confirmed = True
                    write_log_entry(log_id, "Дзен: Публикация подтверждена (текст).")
                    write_log_entry(log_id, f"[dzen] Совпадение: {pat!r}", level='silent')
                    break
            except Exception:
                pass
        if confirmed:
            break

        # 2b. Проверка тост-ошибок Дзена — завершаем сразу, не ждём таймаута
        _check_error_toast()

        # 3. Проверка URL
        url_now = page.url
        if url_now != url_before:
            # state=published / state=pending — Дзен подтвердил публикацию
            if "state=published" in url_now or "state=pending" in url_now:
                state_label = "state=published" if "state=published" in url_now else "state=pending"
                confirmed = True
                write_log_entry(log_id, f"Дзен: URL → {state_label} — публикация подтверждена.")
                break
            video_match = re.search(r"/video/|/shorts/|/watch\?", url_now)
            if video_match or "editor" not in url_now:
                confirmed = True
                write_log_entry(log_id, "Дзен: Публикация подтверждена (смена URL).")
                write_log_entry(log_id, f"[dzen] URL сменился: {url_now}", level='silent')
                break

        page.wait_for_timeout(_CONFIRM_POLL)

    page.remove_listener("framenavigated", _on_navigate)

    if not confirmed:
        # Финальный URL-снимок
        url_after = page.url
        write_log_entry(log_id, f"[dzen] URL до публикации: {url_before}", level='silent')
        write_log_entry(log_id, f"[dzen] URL после публикации: {url_after}", level='silent')
        if "state=published" in url_after or "state=pending" in url_after:
            state_label = "state=published" if "state=published" in url_after else "state=pending"
            confirmed = True
            write_log_entry(log_id, f"Дзен: URL → {state_label} — публикация подтверждена (финал).")
            write_log_entry(log_id, f"[dzen] Полный URL: {url_after}", level='silent')
        else:
            video_url_pattern = re.search(r"/video/|/shorts/|/watch\?", url_after)
            if video_url_pattern and url_after != url_before:
                confirmed = True
                write_log_entry(log_id, "Дзен: Публикация подтверждена (видео-страница).")
                write_log_entry(log_id, f"[dzen] URL видео: {url_after}", level='silent')
            elif url_after != url_before and "editor" not in url_after:
                confirmed = True
                write_log_entry(log_id, "Дзен: Публикация предположительно подтверждена.")
                write_log_entry(log_id, f"[dzen] URL сменился: {url_after}", level='silent')

    _snap(page, batch_id)

    if not confirmed:
        _check_error_toast()  # бросает DzenApiError если есть явная ошибка
        msg = (
            "Подтверждение публикации не получено за 60 с, "
            "но явных ошибок нет — публикация предположительно выполнена"
        )
        write_log_entry(log_id, f"Дзен: {msg}")
        _snap(page, batch_id)

    write_log_entry(log_id, f"[dzen] URL после публикации: {page.url}", level='silent')
    write_log_entry(log_id, "Дзен: Публикация завершена.")
