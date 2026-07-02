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

from clients.common import dismiss_overlays, raise_if_login_required, safe_click
from log import write_log_entry
from utils.utils import fmt_id_msg
from routes.api import publication_file_name, tags

_NAV_TIMEOUT = 60_000   # ms — таймаут одной попытки навигации (1 минута; до 5 попыток подряд)
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
    batch_id,
    category,
    target_id: str | None = None,
    pub_title: str = "",
    batch_session=None,
    keep_browser: bool = False,
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

    write_log_entry(batch_id, category, "Дзен: Публикация запущена.")
    write_log_entry(batch_id, category, fmt_id_msg("[dzen] {} КБ, publisher={}", len(video_data) // 1024, publisher_id), level='silent')

    # Пишем видео во временный файл с именем = заголовок (Дзен автоподставляет имя файла)
    file_name = publication_file_name(pub_title)
    write_log_entry(batch_id, category, f"Заголовок: {pub_title}, файл: {file_name}", level='silent')
    tmp_dir = tempfile.mkdtemp()
    video_path = os.path.join(tmp_dir, file_name)
    try:
        with open(video_path, "wb") as _f:
            _f.write(video_data)

        def _do_publish(page, _ctx):
            _publish_ui(page, publisher_id, video_path, category, batch_id=batch_id)

        result = _get_browser("dzen").run_pipeline_browser(
            _do_publish, saved_cookies, batch_id=batch_id, category=category,
            batch_session=batch_session, keep_browser=keep_browser,
        )

        if not result["ok"]:
            err = result.get("error", "Неизвестная ошибка")
            if "истекла" in err or "авторизуйтесь" in err:
                raise DzenCsrfExpired(err)
            raise DzenApiError(err)

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        if not keep_browser and batch_session is None:
            try:
                _get_browser("dzen").stop(batch_id=batch_id, category=category)
            except Exception:
                pass

    write_log_entry(batch_id, category, "Дзен: видео опубликовано успешно")
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
        write_log_entry(None, 'dzen', f'_snap: {_e}', level='silent')

_CAPTCHA_IFRAME_SELECTOR = (
    "iframe[src*='smartcaptcha'], "
    "iframe[src*='captcha.yandex'], "
    "iframe[src*='not_robot_captcha'], "
    "iframe[src*='yandexcloud'], "
    "iframe[src*='recaptcha'], "
    "iframe[title*='SmartCaptcha'], "
    "iframe[title*='не робот'], "
    "iframe[title*='robot']"
)

_CAPTCHA_FRAME_URL_KEYWORDS = (
    "not_robot_captcha", "smartcaptcha", "yandexcloud",
    "captcha.yandex", "recaptcha",
)

_CAPTCHA_CHECKBOX_SELECTORS = (
    "input[type='checkbox']",
    "[role='checkbox']",
    "[class*='CheckboxCaptcha']",
    "[class*='checkbox-captcha']",
    "[class*='captcha-checkbox']",
    "[class*='Checkbox__box']",
    "div[class*='Checkbox']",
    "span[class*='Checkbox']",
    "label:has-text('не робот')",
)

_CAPTCHA_INLINE_SELECTOR = (
    "[class*='captcha'] input[type='checkbox'], "
    "[class*='captcha'] [role='checkbox'], "
    "[class*='CheckboxCaptcha'], "
    "[class*='captcha-checkbox'], "
    "[id*='captcha'] input[type='checkbox'], "
    "label:has-text('не робот')"
)

def _detect_captcha(page) -> bool:
    """True только если на странице виден виджет капчи (не скрытый iframe в DOM)."""
    try:
        if page.locator(_CAPTCHA_IFRAME_SELECTOR).first.is_visible(timeout=300):
            return True
    except Exception:
        pass
    try:
        if page.locator(_CAPTCHA_INLINE_SELECTOR).first.is_visible(timeout=300):
            return True
    except Exception:
        pass
    return False

def _click_captcha_target(el, batch_id, category, where: str) -> bool:
    try:
        el.click(force=True, timeout=2_000)
        write_log_entry(batch_id, category, f"Капча: Playwright-клик в {where}", level='silent')
        return True
    except Exception:
        pass
    try:
        el.evaluate("el => el.click()")
        write_log_entry(batch_id, category, f"Капча: JS-клик в {where}", level='silent')
        return True
    except Exception:
        return False

def _try_click_captcha_checkbox(page, category, batch_id=None) -> bool:
    """
    Пытается кликнуть чекбокс капчи «Я не робот»:
    frame_locator → все captcha-iframe → inline на странице.
    """
    checkbox_css = ", ".join(_CAPTCHA_CHECKBOX_SELECTORS)

    try:
        captcha_frame = page.frame_locator(_CAPTCHA_IFRAME_SELECTOR).first
        captcha_checkbox = captcha_frame.locator(checkbox_css).first
        captcha_checkbox.wait_for(state="visible", timeout=2_000)
        if _click_captcha_target(captcha_checkbox, batch_id, category, "frame_locator"):
            return True
    except Exception:
        pass

    try:
        for frame in page.frames:
            furl = frame.url.lower()
            is_captcha = any(kw in furl for kw in _CAPTCHA_FRAME_URL_KEYWORDS)
            is_main = furl in ("", "about:blank")
            if not is_captcha and not is_main:
                continue
            for sel in _CAPTCHA_CHECKBOX_SELECTORS:
                try:
                    el = frame.locator(sel).first
                    if el.is_visible(timeout=300):
                        if _click_captcha_target(el, batch_id, category, f"фрейме {furl or 'main'}"):
                            return True
                except Exception:
                    pass
    except Exception:
        pass

    try:
        inline = page.locator(_CAPTCHA_INLINE_SELECTOR).first
        if inline.is_visible(timeout=300):
            if _click_captcha_target(inline, batch_id, category, "inline"):
                return True
    except Exception:
        pass

    return False

def _has_publish_confirm_dialog(page) -> bool:
    """
    Возвращает True если виден вторичный диалог подтверждения.

    Не считаем «Опубликовать после обработки» — это часто основная CTA
    в модалке «Публикация ролика»; автоклик до тегов/комментариев уводит
    в список студии, а дальше скрипт бесконечно ждёт «Опубликовать».
    """
    for text in ("Опубликовать после подтверждения",):
        try:
            btn = page.locator(f"button:has-text('{text}')")
            if btn.count() > 0 and btn.first.is_visible(timeout=300):
                return True
        except Exception:
            pass
    return False

def _detect_confirm_dialog(page) -> bool:
    return _has_publish_confirm_dialog(page)

def _detect_file_input(page) -> bool:
    """input[type=file] в DOM — нативный диалог уже закрыт после set_files(), не трогаем."""
    try:
        return page.locator('input[type="file"]').count() > 0
    except Exception:
        return False

def _handle_captcha_element(page, category, batch_id) -> None:
    """
    Обрабатывает капчу «Я не робот»: кликает чекбокс, ждёт исчезновения.
    Бросает DzenApiError если капча не прошла за 60 сек.
    """
    if not _detect_captcha(page):
        return

    write_log_entry(batch_id, category, "Дзен: Обнаружена капча, пытаюсь нажать «Я не робот».")
    _deadline = _time.monotonic() + 60
    _last_fail_log = 0.0
    while _time.monotonic() < _deadline:
        if _try_click_captcha_checkbox(page, category, batch_id):
            write_log_entry(batch_id, category, "Дзен: Капча — чекбокс нажат, жду исчезновения.")
            _snap(page, batch_id)
            _clear_deadline = _time.monotonic() + 30
            while _time.monotonic() < _clear_deadline:
                page.wait_for_timeout(1_000)
                if not _detect_captcha(page):
                    write_log_entry(batch_id, category, "Дзен: Капча пройдена.")
                    _snap(page, batch_id)
                    return
            _snap(page, batch_id)
            raise DzenApiError("Капча не прошла за 30 сек — публикация невозможна.")

        _now = _time.monotonic()
        if _now - _last_fail_log >= 3:
            write_log_entry(batch_id, category, "Дзен: Капча обнаружена, но кликнуть чекбокс не удалось.")
            _last_fail_log = _now
        page.wait_for_timeout(2_000)

    _snap(page, batch_id)
    raise DzenApiError("Капча не прошла за 60 сек — публикация невозможна.")

def _handle_confirm_element(page, category, batch_id) -> None:
    """Обрабатывает вторичный диалог подтверждения (не основную CTA модалки)."""
    for text in ("Опубликовать после подтверждения",):
        try:
            btn = page.locator(f"button:has-text('{text}')").first
            if btn.is_visible(timeout=300):
                write_log_entry(batch_id, category, f"Дзен: Нажимаю «{text}».")
                write_log_entry(batch_id, category, f"Кнопка подтверждения: «{text}»", level='silent')
                btn.click()
                _snap(page, batch_id)
                return
        except Exception:
            pass

def _dzen_step7_success_without_click(page, url_step7_start: str) -> bool:
    """Признак что публикация уже ушла без финального клика «Опубликовать»."""
    u = page.url
    if "state=published" in u or "state=pending" in u:
        return True
    if re.search(r"/video/|/shorts/|/watch\?", u):
        return True
    if (
        "videoEditorPublicationId" in url_step7_start
        and "videoEditorPublicationId" not in u
    ):
        return True
    return False

def _find_primary_publish_control(page):
    """
    Основная кнопка/ссылка публикации в актуальной вёрстке Дзена
    (текст, роль, data-testid).
    """
    for sel in ('[data-testid="publish-btn"]',):
        try:
            loc = page.locator(sel).first
            if loc.is_visible(timeout=400):
                return loc
        except Exception:
            pass
    for name in ("Опубликовать", "ОПУБЛИКОВАТЬ", "Опубликовать после обработки"):
        for role in ("button", "link"):
            try:
                loc = page.get_by_role(role, name=name).first
                if loc.is_visible(timeout=400):
                    return loc
            except Exception:
                pass
    try:
        loc = page.locator("button").filter(
            has_text=re.compile(r"^\s*Опубликовать\s*$")
        ).first
        if loc.is_visible(timeout=400):
            return loc
    except Exception:
        pass
    for sel in (
        "button:has-text('Опубликовать после обработки')",
        "button:has-text('Опубликовать')",
        "[role='button']:has-text('Опубликовать')",
    ):
        try:
            loc = page.locator(sel).first
            if loc.is_visible(timeout=400):
                return loc
        except Exception:
            pass
    return None

def _dzen_publish_confirmed(page, url_step7_start: str, url_before: str | None = None) -> bool:
    """True если текущий URL подтверждает успешную публикацию."""
    if _dzen_step7_success_without_click(page, url_step7_start):
        return True
    url_now = page.url
    if url_before is not None and url_now == url_before:
        return False
    if "state=published" in url_now or "state=pending" in url_now:
        return True
    if re.search(r"/video/|/shorts/|/watch\?", url_now):
        return True
    if url_before is not None and "editor" not in url_now:
        return True
    return False

def _detect_dzen_publish_workflow(page) -> bool:
    """Рабочая форма публикации — не закрывать через dismiss."""
    if _detect_file_input(page):
        return True
    try:
        if "videoEditorPublicationId" in page.url:
            return True
    except Exception:
        pass
    if _find_primary_publish_control(page) is not None:
        return True
    return False


DZEN_PUBLISH_WHITELIST = [
    ("captcha", _detect_captcha, _handle_captcha_element),
    ("confirm", _detect_confirm_dialog, _handle_confirm_element),
    ("publish_workflow", _detect_dzen_publish_workflow, None),
]


def _dzen_dismiss(page, category, batch_id) -> None:
    dismiss_overlays(page, DZEN_PUBLISH_WHITELIST, batch_id, category, label="Дзен")


def _click_primary_publish_control(page, category, batch_id=None, url_step7_start: str | None = None) -> bool:
    """Закрывает попапы и нажимает основную кнопку «Опубликовать». Возвращает True если кликнули."""
    if url_step7_start and _dzen_publish_confirmed(page, url_step7_start):
        write_log_entry(
            batch_id, category,
            "Дзен: Публикация уже подтверждена по URL — клик не нужен.",
            level='silent',
        )
        return False
    pub_btn = _find_primary_publish_control(page)
    if pub_btn is None:
        return False
    write_log_entry(batch_id, category, "Дзен: Элемент публикации найден, нажимаю.")
    try:
        safe_click(
            pub_btn, page, DZEN_PUBLISH_WHITELIST,
            batch_id=batch_id, category=category, label="Дзен",
            timeout_ms=3_000, max_attempts=5, js_fallback=True,
        )
    except Exception as _click_err:
        write_log_entry(batch_id, category, f"Дзен: Клик «Опубликовать» не прошёл: {_click_err}", level='warn')
        return False
    _snap(page, batch_id)
    if url_step7_start and _dzen_publish_confirmed(page, url_step7_start):
        return False
    return True

def _retry_publish_if_button_visible(page, category, batch_id, url_step7_start, reason: str) -> bool:
    """Повторный клик, если публикация ещё не ушла, а CTA всё ещё на экране."""
    if _dzen_publish_confirmed(page, url_step7_start):
        return False
    if _find_primary_publish_control(page) is None:
        return False
    write_log_entry(batch_id, category, f"Дзен: {reason}")
    return _click_primary_publish_control(page, category, batch_id, url_step7_start)

def _set_comments_all_users(page, category, batch_id=None) -> None:
    """
    Выставляет «Все пользователи» в дропдауне «Кто может комментировать».

    Точная разметка из бандла video-editor 1.8.3 (компоненты nL/zU/_j):
      • Триггер: <button data-testid="select-trigger-button-comment"
                          aria-expanded="true|false">
      • Опция:   элемент с data-testid="<текст опции>"
                 (в _j: `dataTestId: l.content` — т.е. testid = тексту).
                 Например, <... data-testid="Все пользователи" aria-selected=...>
    После клика по опции ОБЯЗАТЕЛЬНО проверяет текст триггера.
    """
    _TARGET = "Все пользователи"
    write_log_entry(batch_id, category, "Дзен: Выставляю «Все пользователи» в «Кто может комментировать».")
    try:
        trigger = page.locator('[data-testid="select-trigger-button-comment"]').first
        try:
            trigger.wait_for(state="visible", timeout=6_000)
        except Exception:
            write_log_entry(
                batch_id, category,
                "Дзен: Триггер select-trigger-button-comment не найден — пропускаю.",
                level='warn',
            )
            return

        current_text = (trigger.inner_text() or "").strip()
        write_log_entry(batch_id, category, f"_set_comments: триггер найден, текущий: {current_text!r}", level='silent')

        if _TARGET in current_text:
            write_log_entry(batch_id, category, "Дзен: Комментарии уже «Все пользователи».", level='silent')
            _snap(page, batch_id)
            return

        # Скроллим триггер ближе к верху, чтобы поповер вместился во viewport.
        try:
            trigger.scroll_into_view_if_needed(timeout=1500)
        except Exception:
            pass
        page.wait_for_timeout(200)

        trigger.click()
        # Пауза, чтобы поповер успел отрендериться и навесить обработчики
        # (по образцу проверенного rutube.py — без неё клик может «промахнуться»).
        page.wait_for_timeout(500)

        # Опция: data-testid в точности равен тексту опции (см. _j в бандле).
        option = page.locator(f'[data-testid="{_TARGET}"]').first
        try:
            option.wait_for(state="visible", timeout=5_000)
            option.click()
            page.wait_for_timeout(500)
        except Exception as _e:
            write_log_entry(
                batch_id, category,
                f"Дзен: Опция [data-testid=\"{_TARGET}\"] не появилась после клика по триггеру.",
                level='warn',
            )
            write_log_entry(batch_id, category, f"Ошибка выбора опции: {_e}", level='silent')
            try:
                page.keyboard.press("Escape")
            except Exception:
                pass
            _snap(page, batch_id)
            return

        # ── Верификация: триггер ДОЛЖЕН теперь показывать «Все пользователи» ──
        page.wait_for_timeout(400)
        try:
            new_text = (trigger.inner_text() or "").strip()
        except Exception:
            new_text = ""
        write_log_entry(batch_id, category, f"_set_comments: триггер ПОСЛЕ установки: {new_text!r}", level='silent')

        if _TARGET in new_text:
            write_log_entry(batch_id, category, "Дзен: Комментарии выставлены «Все пользователи» (подтверждено).")
            _snap(page, batch_id)
        else:
            write_log_entry(
                batch_id, category,
                f"Дзен: НЕ УДАЛОСЬ выставить «Все пользователи» — триггер показывает {new_text!r}.",
                level='warn',
            )
            _snap(page, batch_id)
    except Exception as _e:
        write_log_entry(batch_id, category, "Дзен: Ошибка при настройке комментариев — продолжаю.", level='warn')
        write_log_entry(batch_id, category, f"Ошибка _set_comments_all_users: {_e}", level='silent')

def _publish_ui(page, publisher_id: str, video_path: str, category, batch_id=None):
    """Управляет браузером для публикации видео через UI Дзена."""

    studio_url = f"https://dzen.ru/profile/editor/id/{publisher_id}/"

    # ── Шаг 1: Переходим в студию ────────────────────────────────────────
    write_log_entry(batch_id, category, "Дзен: Переход в студию.")
    write_log_entry(batch_id, category, f"URL студии: {studio_url}", level='silent')
    _last_err = None
    for _attempt in range(1, 6):
        try:
            page.goto(studio_url, wait_until="domcontentloaded", timeout=_NAV_TIMEOUT)
            _last_err = None
            break
        except Exception as _e:
            _last_err = _e
            write_log_entry(
                batch_id, category,
                f"Дзен: попытка {_attempt}/5 перейти в студию не удалась: {_e}",
                level="warn",
            )
            if _attempt < 5:
                _snap(page, batch_id)
    if _last_err is not None:
        raise DzenApiError(
            f"Не удалось перейти в студию Дзена после 5 попыток: {_last_err}"
        ) from _last_err
    _snap(page, batch_id)

    cur = page.url
    write_log_entry(batch_id, category, f"URL после перехода: {cur}", level='silent')

    raise_if_login_required(page, "dzen", publisher_id=publisher_id)

    plus_btn = page.locator(
        "[class*='addButton'], "
        "[class*='author-studio-header__addButton'], "
        "[data-testid='add-publication-button'], "
        "button[aria-label*='Создать'], "
        "button[aria-label*='создать'], "
        "button[title*='Создать'], "
        "button[aria-label*='Create']"
    ).first

    # ── Шаг 2: Кнопка «+» — ждём готовность студии, закрываем модалки ───
    write_log_entry(batch_id, category, "Дзен: Ищу кнопку «+» для создания публикации.")
    _plus_deadline = _time.monotonic() + 180
    _plus_ready = False
    while _time.monotonic() < _plus_deadline:
        raise_if_login_required(page, "dzen", publisher_id=publisher_id)
        _dzen_dismiss(page, category, batch_id)
        try:
            if plus_btn.is_visible(timeout=400):
                _plus_ready = True
                break
        except Exception:
            pass
        page.wait_for_timeout(400)
    if not _plus_ready:
        raise_if_login_required(page, "dzen", publisher_id=publisher_id)
        plus_btn.wait_for(state="visible", timeout=1_000)
    try:
        safe_click(
            plus_btn, page, DZEN_PUBLISH_WHITELIST,
            batch_id=batch_id, category=category, label="Дзен",
            timeout_ms=30_000, max_attempts=5,
        )
    except Exception as _e:
        raise DzenApiError(
            f"Не удалось нажать «+» в студии Дзена: {_e}"
        ) from _e
    write_log_entry(batch_id, category, "Дзен: Кнопка «+» нажата, жду меню.")
    _snap(page, batch_id)

    # Перед шагом 3: закрываем любой неожиданный попап/хинт.
    _dzen_dismiss(page, category, batch_id)

    # ── Шаг 3: «Загрузить видео» из выпадающего меню ─────────────────────
    write_log_entry(batch_id, category, "Дзен: Выбираю «Загрузить видео».")
    upload_item = page.get_by_text("Загрузить видео", exact=True).first
    try:
        upload_item.wait_for(state="visible", timeout=180_000)
    except Exception:
        write_log_entry(batch_id, category, "Дзен: exact-match не нашёл — пробую contains.")
        upload_item = page.locator("text=Загрузить видео").first
        upload_item.wait_for(state="visible", timeout=180_000)
    upload_item.click()
    write_log_entry(batch_id, category, "Дзен: «Загрузить видео» нажато")
    _snap(page, batch_id)

    # Перед шагом 4: закрываем любой неожиданный попап/хинт.
    _dzen_dismiss(page, category, batch_id)

    # ── Шаг 4: Загружаем файл ────────────────────────────────────────────
    write_log_entry(batch_id, category, "Дзен: Ищу поле загрузки файла.")
    # Ждём появления кнопки ДО входа в expect_file_chooser:
    # если войти до того, как кнопка видна, click() зависает внутри with-блока
    # и expect_file_chooser истекает раньше, чем диалог успевает открыться.
    choose_btn = page.get_by_text("Выбрать видео", exact=False).first
    choose_btn.wait_for(state="visible", timeout=180_000)
    write_log_entry(batch_id, category, "Дзен: Кнопка «Выбрать видео» найдена, открываю диалог выбора файла.")
    with page.expect_file_chooser(timeout=180_000) as fc_info:
        choose_btn.click()
    file_chooser = fc_info.value
    file_chooser.set_files(video_path)
    write_log_entry(batch_id, category, "Дзен: Файл передан браузеру, жду загрузки.")
    write_log_entry(batch_id, category, f"Файл: {os.path.basename(video_path)}", level='silent')
    _snap(page, batch_id)

    # Ждём одно из двух:
    #   a) ?videoEditorPublicationId=...  — редактор открылся, нужно кликать «Опубликовать»
    #   b) ?state=published               — Дзен опубликовал сам, ничего больше не нужно
    # Во время ожидания периодически закрываем любые неожиданные попапы.
    write_log_entry(batch_id, category, "Дзен: Жду открытия редактора видео или авто-публикации.")
    _editor_opened = False
    _auto_published = False
    _url_deadline = _time.monotonic() + _UPLOAD_WAIT / 1000
    while _time.monotonic() < _url_deadline:
        _cur = page.url
        if "state=published" in _cur:
            _auto_published = True
            write_log_entry(batch_id, category, "Дзен: Видео опубликовано автоматически.")
            write_log_entry(batch_id, category, f"URL авто-публикации: {_cur}", level='silent')
            break
        if "videoEditorPublicationId" in _cur:
            _editor_opened = True
            write_log_entry(batch_id, category, "Дзен: Редактор видео открылся.")
            write_log_entry(batch_id, category, f"URL редактора: {_cur}", level='silent')
            break
        _dzen_dismiss(page, category, batch_id)
        page.wait_for_timeout(1_500)

    if _auto_published:
        # Видео уже опубликовано — пропускаем шаги 5-9
        _snap(page, batch_id)
        write_log_entry(batch_id, category, "Дзен: Публикация завершена.")
        return

    if not _editor_opened:
        # Запасной вариант: ждём поле заголовка или кнопку в диалоге
        write_log_entry(batch_id, category, "Дзен: URL редактора не появился, жду форму.")
        try:
            page.wait_for_selector(
                "input[placeholder*='аголов'], "
                "textarea[placeholder*='аголов'], "
                "button:has-text('Опубликовать после обработки')",
                timeout=15_000,
            )
        except Exception:
            write_log_entry(batch_id, category, "Дзен: Форма не обнаружена — продолжаю по таймауту.")
            page.wait_for_timeout(5000)
    _snap(page, batch_id)

    # Редактор открылся — закрываем все неожиданные попапы (любые подсказки,
    # хинты, уведомления Дзена), которые могут мешать заполнению формы.
    _dzen_dismiss(page, category, batch_id)

    # ── Шаг 5: Заполняем теги ────────────────────────────────────────────
    write_log_entry(batch_id, category, "Дзен: Заполняю теги.")
    write_log_entry(batch_id, category, f"Теги: {tags()}", level='silent')
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
        write_log_entry(batch_id, category, "Дзен: Теги заполнены")
        _snap(page, batch_id)
    except Exception as _e:
        write_log_entry(batch_id, category, "Дзен: Не удалось заполнить теги — продолжаю.")
        write_log_entry(batch_id, category, f"Ошибка тегов: {_e}", level='silent')

    # Перед шагом 6: закрываем хинт «Уже можно публиковать» (он часто
    # всплывает после ввода тегов и перекрывает дропдаун комментариев).
    _dzen_dismiss(page, category, batch_id)

    # ── Шаг 6: Выставляем «Все пользователи» в «Кто может комментировать» ─
    # Сразу после тегов — контрол виден в той же модалке «Публикация ролика».
    _set_comments_all_users(page, category, batch_id)

    # Перед шагом 7: ещё раз закрываем любые всплывшие хинты/попапы.
    _dzen_dismiss(page, category, batch_id)

    # ── Шаг 7: Публикуем ─────────────────────────────────────────────────
    write_log_entry(
        batch_id, category,
        "Дзен: Жду кнопку публикации или признак что материал уже отправлен.",
    )
    url_step7_start = page.url
    _step7_deadline = _time.monotonic() + 180
    pub_btn = None
    while _time.monotonic() < _step7_deadline:
        if _dzen_step7_success_without_click(page, url_step7_start):
            write_log_entry(
                batch_id, category,
                "Дзен: Публикация уже ушла (редирект/студия) — отдельный клик не нужен.",
            )
            write_log_entry(batch_id, category, f"URL: {page.url}", level='silent')
            pub_btn = None
            break
        pub_btn = _find_primary_publish_control(page)
        if pub_btn is not None:
            break
        _dzen_dismiss(page, category, batch_id)
        page.wait_for_timeout(1_500)

    if pub_btn is None and not _dzen_step7_success_without_click(page, url_step7_start):
        raise DzenApiError(
            "Не дождались кнопки публикации и не обнаружили успешный редирект за 3 минуты."
        )

    if pub_btn is not None:
        _click_primary_publish_control(page, category, batch_id, url_step7_start)

    # Ждём диалог подтверждения или капчу; выходим раньше, если URL уже сменился.
    _CONFIRM_OR_CAPTCHA_SEL = (
        "button:has-text('Опубликовать после подтверждения'), "
        "button:has-text('Опубликовать после обработки'), "
        "iframe[src*='captcha'], iframe[src*='smartcaptcha']"
    )
    _confirm_dialog_deadline = _time.monotonic() + 12
    while _time.monotonic() < _confirm_dialog_deadline:
        if _dzen_step7_success_without_click(page, url_step7_start):
            break
        try:
            if page.locator(_CONFIRM_OR_CAPTCHA_SEL).first.is_visible(timeout=400):
                break
        except Exception:
            pass
        page.wait_for_timeout(400)
    _snap(page, batch_id)

    # ── Шаг 8: Обрабатываем попапы, диалоги, хинты (до 10 секунд) ───────
    # Каждую итерацию вызываем _dzen_dismiss — whitelist (капча, confirm, workflow)
    # либо обрабатывает известный элемент, либо закрывает неизвестный оверлей.
    _DIALOG_WINDOW = 15_000  # ms
    _DIALOG_POLL   = 800     # ms

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
    _step8_done = False

    while _time.monotonic() < _dialog_deadline:
        # Обрабатываем любые попапы/диалоги/хинты через единый список ожидаемых элементов.
        # DzenApiError из _handle_captcha_element пробросится наружу автоматически.
        _dzen_dismiss(page, category, batch_id)

        _check_error_toast()

        if _dzen_step7_success_without_click(page, url_step7_start):
            write_log_entry(
                batch_id, category,
                "Дзен: URL подтверждает публикацию в шаге 8.",
            )
            _step8_done = True
            break

        # Проверяем финальное подтверждение публикации
        try:
            success_now = page.locator(
                "[class*='toast']:has-text('опубликован'), "
                "[class*='notification']:has-text('опубликован'), "
                "[data-testid='publish-success']"
            ).first
            if success_now.is_visible():
                write_log_entry(batch_id, category, "Дзен: Публикация подтверждена в шаге 8.")
                _step8_done = True
                break
        except Exception:
            pass

        page.wait_for_timeout(_DIALOG_POLL)

    write_log_entry(batch_id, category, "Дзен: Шаг 8 завершён, жду подтверждения публикации.")

    # Перед шагом 9: закрываем хинты и повторяем клик, если CTA всё ещё на экране.
    _dzen_dismiss(page, category, batch_id)
    if not _step8_done and not _dzen_publish_confirmed(page, url_step7_start):
        _retry_publish_if_button_visible(
            page,
            category,
            batch_id,
            url_step7_start,
            "Кнопка «Опубликовать» всё ещё видна — повторяю клик после закрытия хинтов.",
        )

    # ── Шаг 9: Ожидаем подтверждения публикации ──────────────────────────
    _PUBLISH_CONFIRM_TIMEOUT = 60_000  # ms — полный таймаут ожидания
    _CONFIRM_POLL = 2_000              # ms — интервал опроса

    url_before = page.url
    confirmed = False

    # Быстрая проверка: браузер уже на странице подтверждения ещё до цикла
    if "state=published" in url_before or "state=pending" in url_before:
        state_label = "state=published" if "state=published" in url_before else "state=pending"
        confirmed = True
        write_log_entry(batch_id, category, f"Дзен: URL → {state_label} — публикация подтверждена.")
        write_log_entry(batch_id, category, f"Полный URL: {url_before}", level='silent')

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
    _publish_retries = 0
    _PUBLISH_RETRY_MAX = 3
    while _time.monotonic() < _confirm_deadline and not confirmed:
        _iter += 1
        if _iter % _snap_every == 1:   # первый снимок сразу, потом каждые 6 сек
            _snap(page, batch_id)

        if _dzen_publish_confirmed(page, url_step7_start, url_before):
            confirmed = True
            url_now = page.url
            if "state=published" in url_now or "state=pending" in url_now:
                state_label = "state=published" if "state=published" in url_now else "state=pending"
                write_log_entry(batch_id, category, f"Дзен: URL → {state_label} — публикация подтверждена.")
            else:
                write_log_entry(batch_id, category, "Дзен: Публикация подтверждена (URL).")
            write_log_entry(batch_id, category, f"URL: {url_now}", level='silent')
            break

        # 1. CSS-проверка
        try:
            el = page.locator(css_success_selector).first
            if el.is_visible():
                confirmed = True
                write_log_entry(batch_id, category, "Дзен: Уведомление об успешной публикации получено (CSS).")
                write_log_entry(batch_id, category, f"URL: {page.url}", level='silent')
                break
        except Exception:
            pass

        # 2. Текстовая проверка
        for pat in text_success_patterns:
            try:
                el = page.locator(pat).first
                if el.is_visible():
                    confirmed = True
                    write_log_entry(batch_id, category, "Дзен: Публикация подтверждена (текст).")
                    write_log_entry(batch_id, category, f"Совпадение: {pat!r}", level='silent')
                    break
            except Exception:
                pass
        if confirmed:
            break

        # 2b. Проверка тост-ошибок Дзена — завершаем сразу, не ждём таймаута
        _check_error_toast()

        # 2c. Обрабатываем попапы/диалоги/хинты (капча может появиться и здесь)
        _dzen_dismiss(page, category, batch_id)

        if _dzen_publish_confirmed(page, url_step7_start, url_before):
            confirmed = True
            url_now = page.url
            if "state=published" in url_now or "state=pending" in url_now:
                state_label = "state=published" if "state=published" in url_now else "state=pending"
                write_log_entry(batch_id, category, f"Дзен: URL → {state_label} — публикация подтверждена.")
            else:
                write_log_entry(batch_id, category, "Дзен: Публикация подтверждена (URL).")
            write_log_entry(batch_id, category, f"URL: {url_now}", level='silent')
            break

        if (
            not confirmed
            and _publish_retries < _PUBLISH_RETRY_MAX
            and _retry_publish_if_button_visible(
                page,
                category,
                batch_id,
                url_step7_start,
                "Повторный клик «Опубликовать» (ожидание подтверждения).",
            )
        ):
            _publish_retries += 1

        page.wait_for_timeout(_CONFIRM_POLL)

    page.remove_listener("framenavigated", _on_navigate)

    if not confirmed:
        # Финальный URL-снимок
        url_after = page.url
        write_log_entry(batch_id, category, f"URL до публикации: {url_before}", level='silent')
        write_log_entry(batch_id, category, f"URL после публикации: {url_after}", level='silent')
        if "state=published" in url_after or "state=pending" in url_after:
            state_label = "state=published" if "state=published" in url_after else "state=pending"
            confirmed = True
            write_log_entry(batch_id, category, f"Дзен: URL → {state_label} — публикация подтверждена (финал).")
            write_log_entry(batch_id, category, f"Полный URL: {url_after}", level='silent')
        else:
            video_url_pattern = re.search(r"/video/|/shorts/|/watch\?", url_after)
            if video_url_pattern and url_after != url_before:
                confirmed = True
                write_log_entry(batch_id, category, "Дзен: Публикация подтверждена (видео-страница).")
                write_log_entry(batch_id, category, f"URL видео: {url_after}", level='silent')
            elif url_after != url_before and "editor" not in url_after:
                confirmed = True
                write_log_entry(batch_id, category, "Дзен: Публикация предположительно подтверждена.")
                write_log_entry(batch_id, category, f"URL сменился: {url_after}", level='silent')

    _snap(page, batch_id)

    if not confirmed:
        _check_error_toast()  # бросает DzenApiError если есть явная ошибка
        _snap(page, batch_id)
        raise DzenApiError(
            "Подтверждение публикации не получено за 60 с — "
            "видео предположительно в черновиках. Проверьте вручную."
        )

    write_log_entry(batch_id, category, f"URL после публикации: {page.url}", level='silent')
    write_log_entry(batch_id, category, "Дзен: Публикация завершена.")
