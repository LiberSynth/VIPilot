"""
Рутьюб-клиент: публикует короткое видео через веб-интерфейс Рутьюба (UI-driven).

Playwright управляет браузером, скриншоты стримятся в виджет «Публикация».
Видео загружается через expect_file_chooser() — как обычный пользователь.
"""

import os
import shutil
import tempfile
import time as _time

from clients.common import (
    element_center_clickable,
    handle_popups,
    noop_dismiss_unknown,
    poll_wait_tick,
    safe_click,
    try_dismiss_publish_overlay,
    wait_for_publish_target,
)
from services.publish_auth_check import raise_if_login_required
from log import write_log_entry
from utils.utils import fmt_id_msg
from routes.api import publication_file_name

_NAV_TIMEOUT  = 60_000   # ms — таймаут одной попытки навигации (1 минута; до 5 попыток подряд)
_UPLOAD_WAIT  = 180_000  # ms — ожидание появления формы публикации (до 3 минут)
_PUBLISH_UI_ATTEMPTS = 3  # полный перезапуск _publish_ui при таймауте кнопки «Опубликовать»
_CATEGORY     = "Юмор"   # категория по умолчанию

STUDIO_URL = "https://studio.rutube.ru/"


def _tn(target_name: str, msg: str) -> str:
    return f"{target_name}: {msg}"


def _is_publish_btn_wait_timeout(exc: BaseException) -> bool:
    """Таймаут ожидания кнопки «Опубликовать» — можно ретраить с нуля."""
    msg = str(exc)
    return "таймаут ожидания" in msg and "Опубликовать" in msg


class RutubeSessionMissing(RuntimeError):
    """Браузерная сессия Рутьюба не сохранена — требуется авторизация."""

class RutubeCsrfExpired(RuntimeError):
    """Сессия истекла — необходима повторная авторизация."""

class RutubeApiError(RuntimeError):
    """Ошибка публикации на Рутьюб."""

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
    target_name: str = "Rutube",
    batch_session=None,
    keep_browser: bool = False,
) -> bool:
    """
    Публикует видео на Рутьюб через веб-интерфейс.
    Браузер виден в панели «Публикация» — можно наблюдать весь процесс.
    Возвращает True при успехе.
    """
    from services.browser_registry import get_browser as _get_browser
    from db import db_get_target_session_context

    cfg = target_config or {}
    person_id = cfg.get("person_id", "")

    if not person_id:
        raise RutubeApiError("person_id не задан в настройках Рутьюб")

    if not target_id:
        raise RutubeSessionMissing("target_id не передан — невозможно загрузить сессию")

    session = db_get_target_session_context(target_id)
    if not session:
        raise RutubeSessionMissing(
            "Браузерная сессия Рутьюб не сохранена — "
            "авторизуйтесь в браузере (вкладка «Публикация»)"
        )

    write_log_entry(batch_id, category, _tn(target_name, "Публикация запущена."))
    write_log_entry(batch_id, category, fmt_id_msg("[rutube] {} КБ, person_id={}", len(video_data) // 1024, person_id), level='silent')

    file_name = publication_file_name(pub_title)
    write_log_entry(batch_id, category, _tn(target_name, f"Заголовок: {pub_title}, файл: {file_name}"), level='silent')
    tmp_dir = tempfile.mkdtemp()
    video_path = os.path.join(tmp_dir, file_name)
    try:
        with open(video_path, "wb") as _f:
            _f.write(video_data)

        def _do_publish(page, ctx):
            for attempt in range(1, _PUBLISH_UI_ATTEMPTS + 1):
                try:
                    if attempt > 1:
                        write_log_entry(
                            batch_id, category,
                            _tn(
                                target_name,
                                "Повтор публикации с начала "
                                f"({attempt}/{_PUBLISH_UI_ATTEMPTS}) "
                                "после таймаута кнопки «Опубликовать».",
                            ),
                            level="warn",
                        )
                        # Short-circuit _ensure_rutube_studio пропускает goto —
                        # на ретрае принудительно уходим с зависшего модала.
                        page.goto(
                            STUDIO_URL,
                            wait_until="domcontentloaded",
                            timeout=_NAV_TIMEOUT,
                        )
                    _publish_ui(
                        page, video_path, category,
                        batch_id=batch_id, ctx=ctx, target_id=target_id,
                        target_name=target_name,
                    )
                    return
                except RutubeApiError as exc:
                    if (
                        not _is_publish_btn_wait_timeout(exc)
                        or attempt >= _PUBLISH_UI_ATTEMPTS
                    ):
                        raise
                    write_log_entry(
                        batch_id, category,
                        _tn(target_name, f"Таймаут кнопки «Опубликовать»: {exc}"),
                        level="warn",
                    )

        result = _get_browser("rutube").run_pipeline_browser(
            _do_publish, target_id, batch_id=batch_id, category=category,
            batch_session=batch_session, keep_browser=keep_browser, target_name=target_name,
        )

        if not result["ok"]:
            err = result.get("error", "Неизвестная ошибка")
            if "истекла" in err or "авторизуйтесь" in err:
                raise RutubeCsrfExpired(err)
            raise RutubeApiError(err)

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        if not keep_browser and batch_session is None:
            try:
                _get_browser("rutube").stop(batch_id=batch_id, category=category)
            except Exception:
                pass

    write_log_entry(batch_id, category, _tn(target_name, "видео опубликовано успешно"))
    return True

# ---------------------------------------------------------------------------
# UI-driven публикация
# ---------------------------------------------------------------------------


def _rutube_category_trigger_visible(page) -> bool:
    try:
        return page.locator("text=Выберите категорию").first.is_visible(timeout=200)
    except Exception:
        return False

def _rutube_upload_form_fields_visible(page) -> bool:
    for sel in (
        "input[placeholder*='азван']",
        "input[placeholder*='азвание']",
        "textarea[placeholder*='азван']",
        "textarea[placeholder*='писан']",
        "textarea[placeholder*='писание']",
    ):
        try:
            if page.locator(sel).first.is_visible(timeout=200):
                return True
        except Exception:
            pass
    return False

def _rutube_upload_form_open(page) -> bool:
    """Модалка публикации открыта — не свёрнутый виджет и не дашборд."""
    if _rutube_category_trigger_visible(page):
        return True
    if not _rutube_publish_button_visible(page):
        return False
    if _rutube_upload_form_fields_visible(page):
        return True
    try:
        if page.get_by_text("Название", exact=True).first.is_visible(timeout=200):
            return True
    except Exception:
        pass
    return False

def _rutube_upload_processing_visible(page) -> bool:
    """Передача файла до открытия формы (не «Обработка N%» в модалке публикации)."""
    if _rutube_upload_form_open(page):
        return False
    for text in ("Загружается", "Загрузка файла", "Идёт загрузка", "Идет загрузка"):
        try:
            if page.get_by_text(text, exact=False).first.is_visible(timeout=200):
                return True
        except Exception:
            pass
    return False

def _rutube_upload_state(page) -> dict:
    """Проверяет видимые признаки загрузки и готовности формы публикации."""
    state = {
        "moderation": False,
        "publish_btn": False,
        "category_trigger": False,
        "uploading": False,
    }
    try:
        state["publish_btn"] = page.locator("button:has-text('Опубликовать')").last.is_visible(timeout=300)
    except Exception:
        pass
    state["category_trigger"] = _rutube_category_trigger_visible(page)
    in_upload_form = state["publish_btn"] or state["category_trigger"]
    try:
        if in_upload_form and page.get_by_text("Модерация", exact=False).first.is_visible(timeout=300):
            state["moderation"] = True
    except Exception:
        pass
    state["uploading"] = _rutube_upload_processing_visible(page)
    return state

def _rutube_upload_ready(page, state: dict) -> bool:
    """Форма открыта — при «Выберите категорию» не ждём «Обработка N%»."""
    if not _rutube_upload_form_open(page):
        return False
    if state["moderation"] or state["category_trigger"]:
        return True
    if state["publish_btn"] and not state["uploading"]:
        return True
    return False

def _find_rutube_add_button(page):
    """Возвращает видимую кнопку «+ Добавить» в студии или None."""
    for loc in (
        page.get_by_role("button", name="+ Добавить"),
        page.get_by_role("button", name="Добавить"),
        page.locator("header button:has-text('Добавить')"),
        page.locator("button:has-text('+ Добавить')"),
        page.locator("[aria-label*='Добавить']"),
    ):
        try:
            candidate = loc.first
            if candidate.is_visible(timeout=300):
                return candidate
        except Exception:
            pass
    return None

def _rutube_add_button_clickable(add_btn) -> bool:
    return element_center_clickable(add_btn)

def _wait_rutube_add_button(page, category, batch_id=None, timeout_ms=180_000, *, target_name: str = "Rutube"):
    """Ждёт готовность студии и видимую кнопку «+ Добавить»."""
    def _add_status(target):
        if target is None:
            return "кнопка «+ Добавить» не найдена."
        if not _rutube_add_button_clickable(target):
            return "кнопка «+ Добавить» перекрыта overlay."
        return "жду готовность студии."

    result = wait_for_publish_target(
        page,
        find_target=lambda: _find_rutube_add_button(page),
        is_ready=lambda t: t is not None and _rutube_add_button_clickable(t),
        whitelist=RUTUBE_PUBLISH_WHITELIST,
        batch_id=batch_id,
        category=category,
        platform="rutube",
        label=target_name,
        timeout_ms=timeout_ms,
        status_message=_add_status,
        before_poll=lambda: raise_if_login_required(page, "rutube"),
        error_factory=RutubeApiError,
        timeout_message="Не дождались кнопки «+ Добавить» в студии Рутьюба.",
    )
    return result

def _wait_rutube_upload(page, category, batch_id=None, *, target_name: str = "Rutube") -> bool:
    write_log_entry(batch_id, category, _tn(target_name, "Жду завершения загрузки (до 3 минут)."))
    deadline = _time.monotonic() + _UPLOAD_WAIT / 1000
    last_log_at = 0.0
    while _time.monotonic() < deadline:
        _rutube_handle_popups(page, category, batch_id, label=target_name)
        state = _rutube_upload_state(page)
        if _rutube_upload_ready(page, state):
            parts = []
            if state["moderation"]:
                parts.append("Модерация")
            if state["publish_btn"]:
                parts.append("Опубликовать")
            if state["category_trigger"]:
                parts.append("категория")
            write_log_entry(
                batch_id, category,
                _tn(
                    target_name,
                    "Загрузка завершена"
                    + (f" ({', '.join(parts)})" if parts else "")
                    + ", перехожу к публикации.",
                ),
            )
            return True

        now = _time.monotonic()
        if now - last_log_at >= 8:
            hint = []
            if state["uploading"]:
                hint.append("идёт загрузка")
            if state["publish_btn"]:
                hint.append("кнопка «Опубликовать» видна")
            if state["moderation"]:
                hint.append("«Модерация» видна")
            if state["category_trigger"]:
                hint.append("поле категории видно")
            msg = ", ".join(hint) if hint else "жду признаки готовности формы"
            write_log_entry(batch_id, category, _tn(target_name, f"Загрузка в процессе — {msg}."))
            last_log_at = now
        poll_wait_tick(page, batch_id, "rutube")

    write_log_entry(batch_id, category, _tn(target_name, "Ожидание загрузки истекло — продолжаю."), level="warn")
    return False

_RUTUBE_PUBLISH_SUCCESS_TEXTS = ("Видео опубликовано", "опубликовано")
_RUTUBE_PUBLISH_ERROR_TEXTS = (
    "Ошибка публикации",
    "не удалось опубликовать",
    "Видео не опубликовано",
    "Произошла ошибка",
)

def _rutube_publish_button_visible(page) -> bool:
    try:
        return page.locator("button:has-text('Опубликовать')").last.is_visible(timeout=300)
    except Exception:
        return False

def _detect_rutube_upload_form(page) -> bool:
    """Whitelist: открытая форма публикации."""
    return _rutube_upload_form_open(page)

def _detect_rutube_upload_menu(page) -> bool:
    """Меню после «+ Добавить» — не закрывать."""
    for text in ("Загрузить видео или Shorts", "Загрузить видео"):
        try:
            if page.get_by_text(text, exact=False).first.is_visible(timeout=200):
                return True
        except Exception:
            pass
    return False

def _detect_rutube_upload_in_progress(page) -> bool:
    """Модал «Выбрать файлы» до открытия формы публикации."""
    if _detect_rutube_upload_form(page):
        return False
    try:
        if page.get_by_text("Выбрать файлы", exact=False).first.is_visible(timeout=200):
            return True
    except Exception:
        pass
    return _rutube_upload_processing_visible(page)

def _detect_rutube_captcha(page) -> bool:
    try:
        if page.locator(
            "iframe[src*='captcha'], iframe[src*='smartcaptcha']",
        ).first.is_visible(timeout=200):
            return True
    except Exception:
        pass
    for text in ("Подтвердите, что вы не робот", "Я не робот"):
        try:
            if page.get_by_text(text, exact=False).first.is_visible(timeout=200):
                return True
        except Exception:
            pass
    return False

def _handle_rutube_captcha(page, category, batch_id) -> None:
    for text in ("Продолжить",):
        try:
            btn = page.get_by_text(text, exact=False).first
            if btn.is_visible(timeout=300):
                btn.click()
                write_log_entry(
                    batch_id, category,
                    "Рутьюб: CAPTCHA-диалог закрыт («Продолжить» нажато).",
                )
                return
        except Exception:
            pass
    write_log_entry(
        batch_id, category,
        "Рутьюб: CAPTCHA обнаружена — требуется ручное прохождение.",
        level="warn",
    )

RUTUBE_PUBLISH_WHITELIST = [
    ("captcha", _detect_rutube_captcha, _handle_rutube_captcha),
    ("upload_in_progress", _detect_rutube_upload_in_progress, None),
    ("upload_form", _detect_rutube_upload_form, None),
    ("upload_menu", _detect_rutube_upload_menu, None),
]

def _rutube_dismiss_unknown(
    page, category, batch_id, *, label: str = "", phase: int = 0, force: bool = False,
    target=None, raise_on_failure: bool = False,
) -> None:
    del phase, force
    try_dismiss_publish_overlay(
        page, RUTUBE_PUBLISH_WHITELIST, batch_id, category,
        target=target, label=label or "Rutube", error_factory=RutubeApiError,
        raise_on_failure=raise_on_failure,
    )

def _rutube_handle_popups(page, category, batch_id, *, label: str = "Rutube") -> None:
    del label
    handle_popups(
        page, RUTUBE_PUBLISH_WHITELIST, noop_dismiss_unknown,
        batch_id, category,
    )

def _rutube_publish_confirmed_after_submit(page) -> bool:
    """Признак успеха после клика: кнопка «Опубликовать» исчезла, студия открыта."""
    if _rutube_publish_button_visible(page):
        return False
    try:
        url = page.url.lower()
    except Exception:
        return False
    return "studio.rutube.ru" in url

def _check_rutube_publish_result(page, *, after_submit: bool = False) -> tuple[bool, str | None]:
    """(True, причина) — публикация подтверждена."""
    try:
        body = page.locator("body").inner_text(timeout=800)
        body_lower = body.lower()
        for err in _RUTUBE_PUBLISH_ERROR_TEXTS:
            if err.lower() in body_lower:
                raise RutubeApiError(f"Рутьюб заблокировал публикацию: «{err}».")
        for ok_text in _RUTUBE_PUBLISH_SUCCESS_TEXTS:
            if ok_text.lower() in body_lower:
                return True, "тост"
    except RutubeApiError:
        raise
    except Exception:
        pass
    if after_submit and _rutube_publish_confirmed_after_submit(page):
        return True, "URL/кнопка"
    return False, None

def _click_rutube_publish_button(pub_btn, page, category, batch_id, *, label: str = "Rutube") -> None:
    """Клик по «Опубликовать» без ожидания навигации после submit."""
    try:
        safe_click(
            pub_btn, page, RUTUBE_PUBLISH_WHITELIST, _rutube_dismiss_unknown,
            batch_id=batch_id, category=category, label=label,
            timeout_ms=2_000, max_attempts=3,
            click_kwargs={"no_wait_after": True},
            js_fallback=True,
        )
    except Exception as _e:
        raise RutubeApiError(
            f"Не удалось нажать «Опубликовать» в Рутьюбе: {_e}"
        ) from _e

def _rutube_publish_button_ready(pub_btn) -> bool:
    try:
        if not pub_btn.is_visible(timeout=300):
            return False
        return not pub_btn.is_disabled(timeout=300)
    except Exception:
        return False

def _submit_rutube_publish(page, category, batch_id, pub_btn=None, *, label: str = "Rutube") -> None:
    """Прокручивает к кнопке, ждёт enabled и нажимает «Опубликовать»."""
    if pub_btn is None:
        pub_btn = page.locator("button:has-text('Опубликовать')").last
    try:
        pub_btn.scroll_into_view_if_needed(timeout=3_000)
    except Exception:
        pass
    page.wait_for_timeout(300)
    _ready_deadline = _time.monotonic() + 8
    while _time.monotonic() < _ready_deadline:
        if _rutube_publish_button_ready(pub_btn):
            break
        page.wait_for_timeout(400)
    _click_rutube_publish_button(pub_btn, page, category, batch_id, label=label)

def _click_rutube_add_button(add_btn, page, category, batch_id, *, label: str = "Rutube") -> None:
    """Клик «+ Добавить» с обходом перекрывающих оверлеев."""
    try:
        add_btn.scroll_into_view_if_needed(timeout=1_000)
    except Exception:
        pass
    try:
        safe_click(
            add_btn, page, RUTUBE_PUBLISH_WHITELIST, _rutube_dismiss_unknown,
            batch_id=batch_id, category=category, label=label,
            timeout_ms=2_000, max_attempts=3, js_fallback=True,
        )
    except Exception as _e:
        raise RutubeApiError(
            f"Не удалось нажать «+ Добавить» в студии Рутьюба: {_e}"
        ) from _e

def _click_rutube_menu_item(upload_item, page, category, batch_id, *, label: str = "Rutube") -> None:
    """Клик пункта меню загрузки с обходом оверлеев."""
    try:
        upload_item.scroll_into_view_if_needed(timeout=1_000)
    except Exception:
        pass
    try:
        safe_click(
            upload_item, page, RUTUBE_PUBLISH_WHITELIST, _rutube_dismiss_unknown,
            batch_id=batch_id, category=category, label=label,
            timeout_ms=2_000, max_attempts=3, js_fallback=True,
        )
    except Exception as _e:
        raise RutubeApiError(
            f"Не удалось выбрать пункт загрузки в Рутьюбе: {_e}"
        ) from _e

def _click_rutube_choose_file(choose_btn, page, category, batch_id, *, label: str = "Rutube") -> None:
    """Клик «Выбрать файлы» с обходом оверлеев."""
    try:
        choose_btn.scroll_into_view_if_needed(timeout=1_000)
    except Exception:
        pass
    try:
        safe_click(
            choose_btn, page, RUTUBE_PUBLISH_WHITELIST, _rutube_dismiss_unknown,
            batch_id=batch_id, category=category, label=label,
            timeout_ms=2_000, max_attempts=3, js_fallback=True,
        )
    except Exception as _e:
        raise RutubeApiError(
            f"Не удалось нажать «Выбрать файлы» в Рутьюбе: {_e}"
        ) from _e

def _click_rutube_locator(locator, page, category, batch_id, *, err_msg: str, label: str = "Rutube") -> None:
    """Клик по элементу формы (категория и т.п.) — safe_click без post-dismiss по форме."""
    try:
        locator.scroll_into_view_if_needed(timeout=1_000)
    except Exception:
        pass
    try:
        safe_click(
            locator, page, RUTUBE_PUBLISH_WHITELIST, _rutube_dismiss_unknown,
            batch_id=batch_id, category=category, label=label,
            timeout_ms=2_000, max_attempts=3, js_fallback=True,
        )
    except Exception as _e:
        raise RutubeApiError(err_msg) from _e

def _ensure_rutube_studio(page, category, batch_id=None, *, target_name: str = "Rutube") -> None:
    """Студия уже открыта bootstrap; повторный goto только если URL не studio."""
    cur = page.url.lower()
    if "studio.rutube.ru" in cur:
        write_log_entry(
            batch_id, category,
            _tn(target_name, f"Студия уже открыта (bootstrap), URL: {page.url}"),
            level="silent",
        )
        return

    write_log_entry(batch_id, category, _tn(target_name, "Переход в студию Рутьюба."))
    _nav_started = _time.monotonic()
    _last_err = None
    for _attempt in range(1, 6):
        try:
            page.goto(STUDIO_URL, wait_until="domcontentloaded", timeout=_NAV_TIMEOUT)
            _last_err = None
            break
        except Exception as _e:
            _last_err = _e
            write_log_entry(
                batch_id, category,
                _tn(target_name, f"попытка {_attempt}/5 перейти в студию не удалась: {_e}"),
                level="warn",
            )
    if _last_err is not None:
        raise RutubeApiError(
            f"Не удалось перейти в студию Рутьюба после 5 попыток: {_last_err}"
        ) from _last_err
    write_log_entry(
        batch_id, category,
        _tn(target_name, f"domcontentloaded за {_time.monotonic() - _nav_started:.1f} с."),
    )

def _publish_ui(
    page,
    video_path: str,
    category,
    batch_id=None,
    *,
    ctx=None,
    target_id=None,
    target_name: str = "Rutube",
):
    """Управляет браузером для публикации видео через UI Рутьюба."""

    _ensure_rutube_studio(page, category, batch_id=batch_id, target_name=target_name)

    cur = page.url
    write_log_entry(batch_id, category, _tn(target_name, f"URL после перехода: {cur}"), level='silent')

    from clients.target_session import refresh_session_after_auth

    refresh_session_after_auth(
        page, ctx, target_id, "rutube",
        batch_id=batch_id, category=category, target_name=target_name,
    )

    _rutube_handle_popups(page, category, batch_id, label=target_name)

    # ── Шаг 2: Кнопка «+ Добавить» ───────────────────────────────────────
    write_log_entry(batch_id, category, _tn(target_name, "Ищу кнопку «+ Добавить»."))
    add_btn = _wait_rutube_add_button(page, category, batch_id=batch_id, target_name=target_name)
    _click_rutube_add_button(add_btn, page, category, batch_id, label=target_name)
    write_log_entry(batch_id, category, _tn(target_name, "Кнопка «+ Добавить» нажата, жду меню."))

    # ── Шаг 3: «Загрузить видео или Shorts» из меню ──────────────────────
    write_log_entry(batch_id, category, _tn(target_name, "Выбираю «Загрузить видео или Shorts»."))
    upload_item = page.get_by_text("Загрузить видео или Shorts", exact=False).first
    try:
        upload_item.wait_for(state="visible", timeout=180_000)
    except Exception:
        write_log_entry(batch_id, category, _tn(target_name, "exact-match не нашёл — пробую contains."))
        upload_item = page.get_by_text("Загрузить видео", exact=False).first
        upload_item.wait_for(state="visible", timeout=180_000)
    _click_rutube_menu_item(upload_item, page, category, batch_id, label=target_name)
    write_log_entry(batch_id, category, _tn(target_name, "«Загрузить видео или Shorts» нажато"))

    # ── Шаг 4: Нажимаем «Выбрать файлы» и передаём видео ────────────────
    write_log_entry(batch_id, category, _tn(target_name, "Ищу поле загрузки файла."))
    choose_btn = page.get_by_text("Выбрать файлы", exact=False).first
    choose_btn.wait_for(state="visible", timeout=180_000)
    write_log_entry(batch_id, category, _tn(target_name, "Кнопка «Выбрать файлы» найдена, открываю диалог выбора файла."))
    with page.expect_file_chooser(timeout=180_000) as fc_info:
        _click_rutube_choose_file(choose_btn, page, category, batch_id, label=target_name)
    file_chooser = fc_info.value
    file_chooser.set_files(video_path)
    write_log_entry(batch_id, category, _tn(target_name, "Файл передан браузеру, жду загрузки."))
    write_log_entry(batch_id, category, _tn(target_name, f"Файл: {os.path.basename(video_path)}"), level='silent')

    # ── Шаг 5: Ждём завершения загрузки ───────────────────────────────────
    _upload_ok = _wait_rutube_upload(page, category, batch_id=batch_id, target_name=target_name)

    # ── Шаг 6: Выбираем категорию ─────────────────────────────────────────
    write_log_entry(batch_id, category, _tn(target_name, f"Выбираю категорию «{_CATEGORY}»."))
    _cat_ok = False
    try:
        if not _detect_rutube_upload_form(page):
            raise RutubeApiError("форма публикации не открыта перед выбором категории")
        cat_trigger = page.locator("text=Выберите категорию").first
        cat_trigger.wait_for(state="visible", timeout=180_000)
        _click_rutube_locator(
            cat_trigger, page, category, batch_id,
            err_msg="Не удалось открыть выбор категории в Рутьюбе",
            label=target_name,
        )
        page.wait_for_timeout(500)
        page.keyboard.type(_CATEGORY)
        page.wait_for_timeout(600)
        cat_option = page.get_by_text(_CATEGORY, exact=True).first
        cat_option.wait_for(state="visible", timeout=5_000)
        _click_rutube_locator(
            cat_option, page, category, batch_id,
            err_msg=f"Не удалось выбрать категорию «{_CATEGORY}» в Рутьюбе",
            label=target_name,
        )
        page.wait_for_timeout(500)
        _cat_ok = True
        write_log_entry(batch_id, category, _tn(target_name, f"Категория «{_CATEGORY}» выбрана"))
    except Exception as _e:
        write_log_entry(batch_id, category, _tn(target_name, "Не удалось выбрать категорию — продолжаю."))
        write_log_entry(batch_id, category, _tn(target_name, f"Ошибка категории: {_e}"), level='silent')

    if not _cat_ok and not _upload_ok:
        raise RutubeApiError(
            "Рутьюб: загрузка не подтверждена и категория не выбрана — "
            "вероятно, сессия устарела или изменился интерфейс"
        )

    if not _cat_ok and not _detect_rutube_upload_form(page):
        raise RutubeApiError(
            "Рутьюб: форма публикации закрыта, категория не выбрана — "
            "вероятно, overlay свернул upload-UI"
        )

    # ── Шаг 7: Нажимаем «Опубликовать» ───────────────────────────────────
    write_log_entry(batch_id, category, _tn(target_name, "Прокручиваю к кнопке «Опубликовать»."))
    if not _detect_rutube_upload_form(page):
        raise RutubeApiError(
            "Рутьюб: форма публикации не открыта — «Опубликовать» недоступна"
        )
    pub_btn = page.locator("button:has-text('Опубликовать')").last
    try:
        pub_btn.wait_for(state="visible", timeout=180_000)
    except Exception as _e:
        raise RutubeApiError(
            "Рутьюб: таймаут ожидания кнопки «Опубликовать»"
        ) from _e

    write_log_entry(batch_id, category, _tn(target_name, "Нажимаю «Опубликовать»."))
    _submit_rutube_publish(page, category, batch_id, pub_btn, label=target_name)

    # ── Шаг 8: Проверяем успех (тост «Видео опубликовано») ──────────────
    write_log_entry(batch_id, category, _tn(target_name, "Проверяю результат публикации."))

    _deadline = _time.monotonic() + 60
    _publish_retries = 0
    _PUBLISH_RETRY_MAX = 3
    _RETRY_INTERVAL = 15
    _last_retry_at = _time.monotonic()
    _success, _success_via = _check_rutube_publish_result(page, after_submit=True)
    while _time.monotonic() < _deadline and not _success:
        page.wait_for_timeout(400)
        _success, _success_via = _check_rutube_publish_result(page, after_submit=True)
        if _success:
            break
        if (
            _publish_retries < _PUBLISH_RETRY_MAX
            and _rutube_publish_button_visible(page)
            and _time.monotonic() - _last_retry_at >= _RETRY_INTERVAL
        ):
            _publish_retries += 1
            _last_retry_at = _time.monotonic()
            write_log_entry(
                batch_id, category,
                _tn(
                    target_name,
                    "Кнопка «Опубликовать» всё ещё видна — повторный клик "
                    f"({_publish_retries}/{_PUBLISH_RETRY_MAX}).",
                ),
            )
            _submit_rutube_publish(page, category, batch_id, label=target_name)


    if _success:
        write_log_entry(
            batch_id, category,
            _tn(target_name, f"Публикация успешна ({_success_via})."),
        )
        write_log_entry(batch_id, category, _tn(target_name, f"URL: {page.url}"), level='silent')
    elif not _upload_ok:
        raise RutubeApiError(
            "Рутьюб: загрузка не подтверждена и публикация не подтверждена — "
            "вероятно, сессия устарела или изменился интерфейс"
        )
    else:
        write_log_entry(
            batch_id, category,
            _tn(target_name, "Публикация не подтверждена после клика «Опубликовать»."),
            level="warn",
        )
        write_log_entry(batch_id, category, _tn(target_name, f"URL: {page.url}"), level='silent')
        raise RutubeApiError(
            "Рутьюб: публикация не подтверждена после клика «Опубликовать»"
        )
