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

from clients.common import (
    _likely_overlay_present,
    click_outside_modal_boundary,
    dismiss_overlay_strict,
    element_click_blocked,
    handle_popups,
    OverlayNotDismissedError,
    poll_wait_tick,
    safe_click,
)
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

        def _do_publish(page, ctx):
            _publish_ui(
                page, publisher_id, video_path, category,
                batch_id=batch_id, ctx=ctx, target_id=target_id,
            )

        result = _get_browser("dzen").run_pipeline_browser(
            _do_publish, target_id, batch_id=batch_id, category=category,
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
    for text in ("Подтвердите, что вы не робот", "Я не робот"):
        try:
            if page.get_by_text(text, exact=False).first.is_visible(timeout=300):
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

def _modal_overlay_visible(page) -> bool:
    try:
        return page.locator("[data-testid='modal-overlay']").first.is_visible(timeout=400)
    except Exception:
        return False

_DZEN_MODAL_CLOSE_SELECTORS = (
    "[class*='modal__rootElement'] button[class*='close']",
    "[class*='modal__rootElement'] [class*='Close']",
    "[class*='modal__rootElement'] button[aria-label*='lose']",
    "[class*='modal__rootElement'] button[aria-label*='закр']",
    "[class*='modal__rootElement'] button[aria-label*='Закр']",
    "button[aria-label*='Закрыть']",
    "button[aria-label*='закрыть']",
    "button[aria-label*='Close']",
    "[class*='toast'] button[class*='close']",
    "[class*='notification'] button[class*='close']",
    "[class*='modal'] button:has-text('Понятно')",
    "[class*='modal'] button:has-text('Не сейчас')",
    "[class*='modal'] button:has-text('Пропустить')",
    "[class*='modal'] button:has-text('Позже')",
)

_DZEN_MODAL_OVERLAY_SELECTOR = "[data-testid='modal-overlay']"
_DZEN_MODAL_ROOT_SELECTOR = "[class*='modal__rootElement']"
_DZEN_HINT_CLOSE_SELECTOR = "[class*='helper-tooltip__closeButton']"


def _dzen_click_outside_modal(page) -> bool:
    return click_outside_modal_boundary(
        page,
        _DZEN_MODAL_OVERLAY_SELECTOR,
        _DZEN_MODAL_ROOT_SELECTOR,
    )


_DZEN_MODAL_DISMISS_EXTRA_STEPS = (
    ("сделан клик за границей окна", _dzen_click_outside_modal),
)

_CONFIRM_OR_CAPTCHA_SEL = (
    "button:has-text('Опубликовать после подтверждения'), "
    "button:has-text('Опубликовать после обработки'), "
    "iframe[src*='captcha'], iframe[src*='smartcaptcha']"
)

_DZEN_SUCCESS_TOAST_SEL = (
    "[class*='toast']:has-text('опубликован'), "
    "[class*='notification']:has-text('опубликован'), "
    "[data-testid='publish-success']"
)

_POST_PUBLISH_POLL_MS = 8_000
_STEP8_WINDOW_MS = 8_000


def dismiss_dzen_hint(
    page,
    category=None,
    batch_id=None,
    *,
    label: str = "Дзен",
    phase: int = 0,
    force: bool = False,
) -> None:
    """Закрывает helper-tooltip хинт Дзена. Без click-outside (ломает меню «+»)."""
    del phase, force
    _user_lvl = "info" if batch_id else "silent"
    _warn_lvl = "warn" if batch_id else "silent"
    prefix = f"{label}: " if label else ""

    hint_was_seen = False
    for _attempt in range(3):
        try:
            btn = page.locator(_DZEN_HINT_CLOSE_SELECTOR).first
            if not btn.is_visible(timeout=300):
                break
        except Exception:
            break

        hint_was_seen = True

        try:
            cls_before = btn.get_attribute("class", timeout=300) or ""
        except Exception:
            cls_before = ""

        write_log_entry(
            batch_id, category,
            f"{prefix}сделан клик по кнопке хинта (попытка {_attempt + 1})",
            level=_user_lvl,
        )
        write_log_entry(
            batch_id, category,
            f"hint close target class={cls_before!r}",
            level="silent",
        )

        try:
            url_before_click = page.url
        except Exception:
            url_before_click = ""

        try:
            btn.click(timeout=2_000)
        except Exception as _e:
            write_log_entry(
                batch_id, category, f"hint click failed: {_e}", level="silent",
            )
            try:
                url_now = page.url
            except Exception:
                url_now = ""
            try:
                still_visible = page.locator(
                    _DZEN_HINT_CLOSE_SELECTOR,
                ).first.is_visible(timeout=200)
            except Exception:
                still_visible = False

            if not still_visible:
                write_log_entry(
                    batch_id, category,
                    f"{prefix}Оверлей закрыт.",
                    level=_user_lvl,
                )
                return

            left_editor = (
                ("videoEditorPublicationId" in (url_before_click or ""))
                and ("videoEditorPublicationId" not in (url_now or ""))
            ) or ("state=published" in (url_now or "")) or ("state=pending" in (url_now or ""))
            if left_editor:
                write_log_entry(
                    batch_id, category,
                    f"hint close interrupted by navigation: {url_now}",
                    level="silent",
                )
                return

            write_log_entry(
                batch_id, category,
                "hint click failed, retrying.",
                level="silent",
            )
            continue

        page.wait_for_timeout(300)

        try:
            still_visible = page.locator(
                _DZEN_HINT_CLOSE_SELECTOR,
            ).first.is_visible(timeout=200)
        except Exception:
            still_visible = False

        if not still_visible:
            write_log_entry(
                batch_id, category,
                f"{prefix}Оверлей закрыт.",
                level=_user_lvl,
            )
            return

        write_log_entry(
            batch_id, category,
            "хинт всё ещё виден после клика — повтор.",
            level="silent",
        )

    if hint_was_seen:
        write_log_entry(
            batch_id, category,
            f"{prefix}Оверлей не закрылся за 3 попытки.",
            level=_warn_lvl,
        )

# ---------------------------------------------------------------------------
# Известные ожидаемые элементы — список признаков и действий
#
# Каждая запись: (имя, detect(page)->bool, handle(page, category, batch_id)->None)
# handle=None означает «обнаружен, действий не требует — просто не закрывать».
# Всё, что не попало в whitelist — dismiss через _dzen_dismiss_unknown (hint).
# ---------------------------------------------------------------------------

def _detect_confirm_dialog(page) -> bool:
    return _has_publish_confirm_dialog(page)

def _detect_dzen_upload_modal(page) -> bool:
    """Модал выбора файла — тот же modal-overlay, что у донатов."""
    for text in (
        "Выбрать видео", "Выберите видео",
        "Перетащите", "Загрузите видео",
    ):
        try:
            if page.get_by_text(text, exact=False).first.is_visible(timeout=200):
                return True
        except Exception:
            pass
    return False

def _detect_dzen_upload_in_progress(page) -> bool:
    """Загрузка файла после set_files, до URL редактора — не dismiss."""
    try:
        if "videoEditorPublicationId" in page.url:
            return False
    except Exception:
        pass
    if not _modal_overlay_visible(page):
        return False
    if _detect_dzen_upload_modal(page):
        return True
    for text in (
        "загружа", "Загрузка", "обработ", "Передач", "Подождите",
    ):
        try:
            if page.get_by_text(text, exact=False).first.is_visible(timeout=150):
                return True
        except Exception:
            pass
    return False

def _detect_dzen_publish_editor(page) -> bool:
    """Редактор / модал «Публикация ролика» — рабочий UI, не мусор."""
    try:
        if "videoEditorPublicationId" in page.url:
            return True
    except Exception:
        pass
    for text in ("Публикация ролика", "Опубликовать после обработки"):
        try:
            if page.get_by_text(text, exact=False).first.is_visible(timeout=200):
                return True
        except Exception:
            pass
    for sel in (
        "input[placeholder*='теги']",
        "input[placeholder*='Теги']",
        '[data-testid="select-trigger-button-comment"]',
        '[data-testid="publish-btn"]',
    ):
        try:
            if page.locator(sel).first.is_visible(timeout=200):
                return True
        except Exception:
            pass
    return False

def _detect_dzen_create_menu(page) -> bool:
    """Выпадающее меню «+» с пунктом «Загрузить видео»."""
    try:
        return page.get_by_text("Загрузить видео", exact=True).first.is_visible(timeout=200)
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
            _clear_deadline = _time.monotonic() + 30
            while _time.monotonic() < _clear_deadline:
                page.wait_for_timeout(1_000)
                if not _detect_captcha(page):
                    write_log_entry(batch_id, category, "Дзен: Капча пройдена.")
                    return
            raise DzenApiError("Капча не прошла за 30 сек — публикация невозможна.")

        _now = _time.monotonic()
        if _now - _last_fail_log >= 3:
            write_log_entry(batch_id, category, "Дзен: Капча обнаружена, но кликнуть чекбокс не удалось.")
            _last_fail_log = _now
        page.wait_for_timeout(2_000)

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

def _confirm_or_captcha_visible(page) -> bool:
    try:
        return page.locator(_CONFIRM_OR_CAPTCHA_SEL).first.is_visible(timeout=150)
    except Exception:
        return False

def _dzen_publish_success_toast_visible(page) -> bool:
    try:
        return page.locator(_DZEN_SUCCESS_TOAST_SEL).first.is_visible(timeout=150)
    except Exception:
        return False

_DZEN_SUCCESS_BODY_PHRASES = (
    "видео опубликовано",
    "ролик опубликован",
    "отправлено на модерацию",
    "видео на модерации",
    "будет опубликовано",
    "видео добавлено",
    "видео обрабатывается",
)

def _dzen_post_submit_success_visible(page) -> bool:
    """Публикация уже ушла: тост, текст на странице или список «Опубликованные»."""
    if _dzen_publish_success_toast_visible(page):
        return True
    try:
        body_lower = page.locator("body").inner_text(timeout=500).lower()
    except Exception:
        body_lower = ""
    for phrase in _DZEN_SUCCESS_BODY_PHRASES:
        if phrase in body_lower:
            return True
    try:
        url = page.url
    except Exception:
        return False
    if "videoEditorPublicationId" in url:
        return False
    try:
        if (
            page.get_by_text("Опубликованные", exact=False).first.is_visible(timeout=150)
            and page.get_by_text("Опубликовано", exact=False).first.is_visible(timeout=150)
        ):
            return True
    except Exception:
        pass
    return False

def _dzen_publish_settled(page, url_step7_start: str) -> bool:
    return (
        _dzen_publish_confirmed(page, url_step7_start)
        or _dzen_step7_success_without_click(page, url_step7_start)
        or _dzen_publish_success_toast_visible(page)
        or _dzen_post_submit_success_visible(page)
    )

def _click_primary_publish_control(page, category, batch_id=None, url_step7_start: str | None = None) -> bool:
    """Закрывает попапы и нажимает основную кнопку «Опубликовать». Возвращает True если кликнули."""
    if url_step7_start and _dzen_publish_settled(page, url_step7_start):
        write_log_entry(
            batch_id, category,
            "Дзен: Публикация уже подтверждена — клик не нужен.",
            level='silent',
        )
        return False
    _dzen_handle_popups(page, category, batch_id)
    pub_btn = _find_primary_publish_control(page)
    if pub_btn is None:
        return False
    write_log_entry(batch_id, category, "Дзен: Элемент публикации найден, нажимаю.")
    try:
        safe_click(
            pub_btn, page, DZEN_PUBLISH_WHITELIST, _dzen_dismiss_unknown,
            batch_id=batch_id, category=category, label="Дзен",
            timeout_ms=3_000, max_attempts=3, js_fallback=True,
        )
    except Exception as _click_err:
        if url_step7_start and _dzen_publish_settled(page, url_step7_start):
            write_log_entry(
                batch_id, category,
                "Дзен: Клик не прошёл, но публикация уже подтверждена.",
            )
            return False
        if _dzen_post_submit_success_visible(page):
            write_log_entry(
                batch_id, category,
                "Дзен: Клик заблокирован — на странице уже признак успешной публикации.",
            )
            return False
        write_log_entry(batch_id, category, f"Клик «Опубликовать» не прошёл: {_click_err}", level='silent')
        return False
    if url_step7_start and _dzen_publish_settled(page, url_step7_start):
        return False
    return True

def _poll_after_publish_click(
    page, category, batch_id, url_step7_start: str, *, timeout_ms: int = _POST_PUBLISH_POLL_MS,
) -> None:
    """Короткий poll после клика: успех, confirm/captcha или повтор CTA."""
    deadline = _time.monotonic() + timeout_ms / 1000
    while _time.monotonic() < deadline:
        if _dzen_publish_settled(page, url_step7_start):
            return
        if _confirm_or_captcha_visible(page) or _detect_captcha(page):
            return
        if _dzen_post_submit_success_visible(page):
            return
        if _find_primary_publish_control(page) is not None:
            _retry_publish_if_button_visible(
                page, category, batch_id, url_step7_start,
                "Кнопка «Опубликовать» всё ещё видна — повторяю клик.",
            )
            return
        poll_wait_tick(page, batch_id, "dzen")

def _retry_publish_if_button_visible(page, category, batch_id, url_step7_start, reason: str) -> bool:
    """Повторный клик, если публикация ещё не ушла, а CTA всё ещё на экране."""
    if _dzen_publish_settled(page, url_step7_start):
        return False
    if _find_primary_publish_control(page) is None:
        return False
    write_log_entry(batch_id, category, f"Дзен: {reason}")
    return _click_primary_publish_control(page, category, batch_id, url_step7_start)

def _dzen_target_blocked(page) -> bool:
    """Целевой UI редактора перекрыт мусором (без каталога попапов)."""
    if not _detect_dzen_publish_editor(page):
        return False
    pub = _find_primary_publish_control(page)
    if pub is not None and element_click_blocked(pub):
        return True
    try:
        trigger = page.locator('[data-testid="select-trigger-button-comment"]').first
        if trigger.is_visible(timeout=150) and element_click_blocked(trigger):
            return True
    except Exception:
        pass
    try:
        if page.locator("[role='alert']").first.is_visible(timeout=150):
            return True
    except Exception:
        pass
    return False

DZEN_PUBLISH_WHITELIST = [
    ("captcha", _detect_captcha, _handle_captcha_element),
    ("upload_modal", _detect_dzen_upload_modal, None),
    ("upload_in_progress", _detect_dzen_upload_in_progress, None),
    ("publish_editor", _detect_dzen_publish_editor, None),
    ("create_menu", _detect_dzen_create_menu, None),
    ("confirm", _detect_confirm_dialog, _handle_confirm_element),
    # file_input НЕ ДОБАВЛЯТЬ сюда — после set_files() input[type=file] остаётся в DOM
    # на всё время публикации и блокирует dismiss для любых других попапов.
    # modal-overlay (донаты и пр.) — мусор, закрывается через dismiss_overlay_strict.
]

def _dzen_whitelisted_overlay_present(page) -> bool:
    for _name, detect, _handle in DZEN_PUBLISH_WHITELIST:
        try:
            if detect(page):
                return True
        except Exception:
            pass
    return False

def _dzen_garbage_overlay_present(page) -> bool:
    """Мусор поверх студии; whitelisted modal-overlay / рабочие модалки — не мусор."""
    if _dzen_target_blocked(page):
        return True
    if _dzen_whitelisted_overlay_present(page):
        return False
    if _modal_overlay_visible(page):
        return True
    return _likely_overlay_present(page)

def _dzen_dismiss_unknown(
    page, category, batch_id, *, label: str = "", phase: int = 0, force: bool = False,
) -> None:
    del phase, force
    lbl = label or "Дзен"
    if _detect_captcha(page):
        return
    if _dzen_garbage_overlay_present(page):
        try:
            dismiss_overlay_strict(
                page, category, batch_id, label=lbl,
                is_present=_dzen_garbage_overlay_present,
                extra_close_selectors=_DZEN_MODAL_CLOSE_SELECTORS,
                extra_steps=_DZEN_MODAL_DISMISS_EXTRA_STEPS,
            )
        except OverlayNotDismissedError as exc:
            raise DzenApiError(str(exc)) from exc
        return
    dismiss_dzen_hint(page, category, batch_id, label=lbl)

def _dzen_handle_popups(
    page, category=None, batch_id=None, *, allow_dismiss: bool = True,
) -> None:
    had_whitelisted = _dzen_whitelisted_overlay_present(page)
    handle_popups(
        page, DZEN_PUBLISH_WHITELIST, _dzen_dismiss_unknown,
        batch_id, category, allow_dismiss=allow_dismiss,
    )
    # publish_editor/create_menu в whitelist блокируют dismiss_unknown.
    if not allow_dismiss:
        dismiss_dzen_hint(page, category, batch_id)
        return
    if had_whitelisted:
        _dzen_dismiss_unknown(page, category, batch_id, label="Дзен")

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
        else:
            write_log_entry(
                batch_id, category,
                f"Дзен: НЕ УДАЛОСЬ выставить «Все пользователи» — триггер показывает {new_text!r}.",
                level='warn',
            )
    except Exception as _e:
        write_log_entry(batch_id, category, "Дзен: Ошибка при настройке комментариев — продолжаю.", level='warn')
        write_log_entry(batch_id, category, f"Ошибка _set_comments_all_users: {_e}", level='silent')

def _publish_ui(
    page,
    publisher_id: str,
    video_path: str,
    category,
    batch_id=None,
    *,
    ctx=None,
    target_id=None,
):
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
    if _last_err is not None:
        raise DzenApiError(
            f"Не удалось перейти в студию Дзена после 5 попыток: {_last_err}"
        ) from _last_err

    cur = page.url
    write_log_entry(batch_id, category, f"URL после перехода: {cur}", level='silent')
    from clients.target_session import refresh_session_after_auth
    from services.publish_auth_check import raise_if_login_required

    refresh_session_after_auth(
        page, ctx, target_id, "dzen",
        batch_id=batch_id, category=category, publisher_id=publisher_id,
    )

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
        _dzen_handle_popups(page, category, batch_id)
        if _dzen_garbage_overlay_present(page):
            page.wait_for_timeout(400)
            continue
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
    _last_plus_err = None
    try:
        safe_click(
            plus_btn, page, DZEN_PUBLISH_WHITELIST, _dzen_dismiss_unknown,
            batch_id=batch_id, category=category, label="Дзен",
            timeout_ms=2_000, max_attempts=5, js_fallback=True,
        )
    except Exception as _e:
        _last_plus_err = _e
    if _last_plus_err is not None:
        raise DzenApiError(
            f"Не удалось нажать «+» в студии Дзена: {_last_plus_err}"
        ) from _last_plus_err
    write_log_entry(batch_id, category, "Дзен: Кнопка «+» нажата, жду меню.")

    # Перед шагом 3: закрываем любой неожиданный попап/хинт.
    _dzen_handle_popups(page, category, batch_id)

    # ── Шаг 3: «Загрузить видео» из выпадающего меню ─────────────────────
    write_log_entry(batch_id, category, "Дзен: Выбираю «Загрузить видео».")
    upload_item = page.get_by_text("Загрузить видео", exact=True).first
    try:
        upload_item.wait_for(state="visible", timeout=180_000)
    except Exception:
        write_log_entry(batch_id, category, "Дзен: exact-match не нашёл — пробую contains.")
        upload_item = page.locator("text=Загрузить видео").first
        upload_item.wait_for(state="visible", timeout=180_000)
    safe_click(
        upload_item, page, DZEN_PUBLISH_WHITELIST, _dzen_dismiss_unknown,
        batch_id=batch_id, category=category, label="Дзен",
        timeout_ms=2_000, max_attempts=3, js_fallback=True,
    )
    write_log_entry(batch_id, category, "Дзен: «Загрузить видео» нажато")

    # Перед шагом 4: закрываем любой неожиданный попап/хинт.
    _dzen_handle_popups(page, category, batch_id)

    # ── Шаг 4: Загружаем файл ────────────────────────────────────────────
    write_log_entry(batch_id, category, "Дзен: Ищу поле загрузки файла.")
    # Ждём появления кнопки ДО входа в expect_file_chooser:
    # если войти до того, как кнопка видна, click() зависает внутри with-блока
    # и expect_file_chooser истекает раньше, чем диалог успевает открыться.
    choose_btn = page.get_by_text("Выбрать видео", exact=False).first
    choose_btn.wait_for(state="visible", timeout=180_000)
    write_log_entry(batch_id, category, "Дзен: Кнопка «Выбрать видео» найдена, открываю диалог выбора файла.")
    with page.expect_file_chooser(timeout=180_000) as fc_info:
        safe_click(
            choose_btn, page, DZEN_PUBLISH_WHITELIST, _dzen_dismiss_unknown,
            batch_id=batch_id, category=category, label="Дзен",
            timeout_ms=2_000, max_attempts=3, js_fallback=True,
        )
    file_chooser = fc_info.value
    file_chooser.set_files(video_path)
    write_log_entry(batch_id, category, "Дзен: Файл передан браузеру, жду загрузки.")
    write_log_entry(batch_id, category, f"Файл: {os.path.basename(video_path)}", level='silent')

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
        _dzen_handle_popups(page, category, batch_id, allow_dismiss=False)
        poll_wait_tick(page, batch_id, "dzen")

    if _auto_published:
        # Видео уже опубликовано — пропускаем шаги 5-9
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

    # Редактор открылся — закрываем все неожиданные попапы (любые подсказки,
    # хинты, уведомления Дзена), которые могут мешать заполнению формы.
    _dzen_handle_popups(page, category, batch_id)

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
    except Exception as _e:
        write_log_entry(batch_id, category, "Дзен: Не удалось заполнить теги — продолжаю.")
        write_log_entry(batch_id, category, f"Ошибка тегов: {_e}", level='silent')

    # Перед шагом 6: закрываем хинт «Уже можно публиковать» (он часто
    # всплывает после ввода тегов и перекрывает дропдаун комментариев).
    _dzen_handle_popups(page, category, batch_id)

    # ── Шаг 6: Выставляем «Все пользователи» в «Кто может комментировать» ─
    # Сразу после тегов — контрол виден в той же модалке «Публикация ролика».
    _set_comments_all_users(page, category, batch_id)

    # Перед шагом 7: ещё раз закрываем любые всплывшие хинты/попапы.
    _dzen_handle_popups(page, category, batch_id)

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
        _dzen_handle_popups(page, category, batch_id)
        poll_wait_tick(page, batch_id, "dzen")

    if pub_btn is None and not _dzen_step7_success_without_click(page, url_step7_start):
        raise DzenApiError(
            "Не дождались кнопки публикации и не обнаружили успешный редирект за 3 минуты."
        )

    if pub_btn is not None:
        _click_primary_publish_control(page, category, batch_id, url_step7_start)
        if not _dzen_publish_settled(page, url_step7_start):
            _poll_after_publish_click(page, category, batch_id, url_step7_start)

    # ── Шаг 8: captcha / confirm / хинты (короткое окно) ─────────────────
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
            body = page.locator("body").inner_text(timeout=400)
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

    _dialog_deadline = _time.monotonic() + _STEP8_WINDOW_MS / 1000
    _step8_done = False
    _step8_iter = 0

    while _time.monotonic() < _dialog_deadline:
        _step8_iter += 1
        _dzen_handle_popups(page, category, batch_id)

        if _step8_iter % 3 == 0:
            _check_error_toast()

        if _dzen_publish_settled(page, url_step7_start):
            write_log_entry(
                batch_id, category,
                "Дзен: Публикация подтверждена в шаге 8.",
            )
            _step8_done = True
            break

        if _confirm_or_captcha_visible(page) or _detect_captcha(page):
            poll_wait_tick(page, batch_id, "dzen")
            continue

        if _dzen_post_submit_success_visible(page):
            write_log_entry(
                batch_id, category,
                "Дзен: Публикация уже отображается как успешная — шаг 8 завершён.",
            )
            _step8_done = True
            break

        if _find_primary_publish_control(page) is not None:
            _retry_publish_if_button_visible(
                page, category, batch_id, url_step7_start,
                "Кнопка «Опубликовать» всё ещё видна — повторяю клик в шаге 8.",
            )
            poll_wait_tick(page, batch_id, "dzen")
            continue

        break

    write_log_entry(batch_id, category, "Дзен: Шаг 8 завершён, жду подтверждения публикации.")

    _dzen_handle_popups(page, category, batch_id)
    if not _step8_done and not _dzen_publish_settled(page, url_step7_start):
        _retry_publish_if_button_visible(
            page,
            category,
            batch_id,
            url_step7_start,
            "Кнопка «Опубликовать» всё ещё видна — повторяю клик после закрытия хинтов.",
        )

    # ── Шаг 9: Ожидаем подтверждения публикации ──────────────────────────
    _PUBLISH_CONFIRM_TIMEOUT = 60_000  # ms — полный таймаут ожидания

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

    _confirm_deadline = _time.monotonic() + _PUBLISH_CONFIRM_TIMEOUT / 1000
    _iter = 0
    _publish_retries = 0
    _PUBLISH_RETRY_MAX = 3
    while _time.monotonic() < _confirm_deadline and not confirmed:
        _iter += 1

        if _dzen_publish_settled(page, url_step7_start):
            confirmed = True
            url_now = page.url
            if "state=published" in url_now or "state=pending" in url_now:
                state_label = "state=published" if "state=published" in url_now else "state=pending"
                write_log_entry(batch_id, category, f"Дзен: URL → {state_label} — публикация подтверждена.")
            else:
                write_log_entry(batch_id, category, "Дзен: Публикация подтверждена (тост/settled).")
            write_log_entry(batch_id, category, f"URL: {url_now}", level='silent')
            break

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
        _dzen_handle_popups(page, category, batch_id)

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

        poll_wait_tick(page, batch_id, "dzen")

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


    if not confirmed:
        _check_error_toast()  # бросает DzenApiError если есть явная ошибка
        raise DzenApiError(
            "Подтверждение публикации не получено за 60 с — "
            "видео предположительно в черновиках. Проверьте вручную."
        )

    write_log_entry(batch_id, category, f"URL после публикации: {page.url}", level='silent')
    write_log_entry(batch_id, category, "Дзен: Публикация завершена.")
