"""
Рутьюб-клиент: публикует короткое видео через веб-интерфейс Рутьюба (UI-driven).

Playwright управляет браузером, скриншоты стримятся в виджет «Публикация».
Видео загружается через expect_file_chooser() — как обычный пользователь.
"""

import os
import shutil
import tempfile
import time as _time

from log import write_log_entry
from utils.utils import fmt_id_msg
from routes.api import publication_file_name

_NAV_TIMEOUT  = 60_000   # ms — таймаут одной попытки навигации (1 минута; до 5 попыток подряд)
_UPLOAD_WAIT  = 180_000  # ms — ожидание завершения загрузки (до 3 минут)
_CATEGORY     = "Юмор"   # категория по умолчанию

STUDIO_URL = "https://studio.rutube.ru/"

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

    saved_cookies = session.get("cookies", [])

    write_log_entry(batch_id, category, "Рутьюб: Публикация запущена.")
    write_log_entry(batch_id, category, fmt_id_msg("[rutube] {} КБ, person_id={}", len(video_data) // 1024, person_id), level='silent')

    file_name = publication_file_name(pub_title)
    write_log_entry(batch_id, category, f"Заголовок: {pub_title}, файл: {file_name}", level='silent')
    tmp_dir = tempfile.mkdtemp()
    video_path = os.path.join(tmp_dir, file_name)
    try:
        with open(video_path, "wb") as _f:
            _f.write(video_data)

        def _do_publish(page, _ctx):
            _publish_ui(page, video_path, category, batch_id=batch_id)

        result = _get_browser("rutube").run_pipeline_browser(
            _do_publish, saved_cookies, batch_id=batch_id, category=category,
            batch_session=batch_session, keep_browser=keep_browser,
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

    write_log_entry(batch_id, category, "Рутьюб: видео опубликовано успешно")
    return True

# ---------------------------------------------------------------------------
# UI-driven публикация
# ---------------------------------------------------------------------------

def _snap(page, batch_id=None) -> None:
    """Снимает скриншот и передаёт кадр в SSE-трансляцию и монитор (thread-safe)."""
    try:
        from services.browser_registry import get_browser as _get_browser
        _b = _get_browser("rutube")
        img = page.screenshot(type="jpeg", quality=65)
        _b.push_frame(img)
        if batch_id:
            _b.push_frame_for_batch(batch_id, img)
    except Exception as _e:
        write_log_entry(None, 'rutube', f'_snap: {_e}', level='silent')

def _rutube_upload_state(page) -> dict:
    """Проверяет видимые признаки загрузки и готовности формы публикации."""
    state = {
        "moderation": False,
        "publish_btn": False,
        "category_trigger": False,
        "uploading": False,
    }
    try:
        state["moderation"] = page.get_by_text("Модерация", exact=False).first.is_visible(timeout=300)
    except Exception:
        pass
    try:
        state["publish_btn"] = page.locator("button:has-text('Опубликовать')").last.is_visible(timeout=300)
    except Exception:
        pass
    try:
        state["category_trigger"] = page.locator("text=Выберите категорию").first.is_visible(timeout=300)
    except Exception:
        pass
    try:
        body = page.locator("body").inner_text(timeout=1000).lower()
        for marker in (
            "загружается", "загрузка файла", "загрузка видео",
            "идёт загрузка", "идет загрузка", "uploading",
        ):
            if marker in body:
                state["uploading"] = True
                break
    except Exception:
        pass
    return state

def _rutube_upload_ready(state: dict) -> bool:
    if state["moderation"]:
        return True
    if state["publish_btn"] and state["category_trigger"]:
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

def _wait_rutube_add_button(page, category, batch_id=None, timeout_ms=180_000):
    """Ждёт готовность студии и видимую кнопку «+ Добавить»."""
    from services.publish_auth_check import raise_if_login_required

    deadline = _time.monotonic() + timeout_ms / 1000
    last_snap_at = 0.0
    while _time.monotonic() < deadline:
        raise_if_login_required(page, "rutube")
        add_btn = _find_rutube_add_button(page)
        if add_btn is not None:
            return add_btn
        now = _time.monotonic()
        if now - last_snap_at >= 5:
            _snap(page, batch_id)
            last_snap_at = now
        page.wait_for_timeout(500)
    raise_if_login_required(page, "rutube")
    raise RutubeApiError("Не дождались кнопки «+ Добавить» в студии Рутьюба.")

def _wait_rutube_upload(page, category, batch_id=None) -> bool:
    write_log_entry(batch_id, category, "Рутьюб: Жду завершения загрузки (до 3 минут).")
    deadline = _time.monotonic() + _UPLOAD_WAIT / 1000
    last_log_at = 0.0
    last_snap_at = 0.0
    while _time.monotonic() < deadline:
        state = _rutube_upload_state(page)
        if _rutube_upload_ready(state):
            parts = []
            if state["moderation"]:
                parts.append("Модерация")
            if state["publish_btn"]:
                parts.append("Опубликовать")
            if state["category_trigger"]:
                parts.append("категория")
            write_log_entry(
                batch_id, category,
                "Рутьюб: Загрузка завершена"
                + (f" ({', '.join(parts)})" if parts else "")
                + ", перехожу к публикации.",
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
            write_log_entry(batch_id, category, f"Рутьюб: Загрузка в процессе — {msg}.")
            last_log_at = now
        if now - last_snap_at >= 5:
            _snap(page, batch_id)
            last_snap_at = now

        page.wait_for_timeout(800)

    write_log_entry(batch_id, category, "Рутьюб: Ожидание загрузки истекло — продолжаю.", level="warn")
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

def _click_rutube_publish_button(pub_btn, page, category, batch_id) -> None:
    """Клик по «Опубликовать» без ожидания навигации после submit."""
    _last_err = None
    for _attempt in range(1, 4):
        try:
            pub_btn.click(timeout=5_000, no_wait_after=True)
            return
        except Exception as _e:
            _last_err = _e
            try:
                pub_btn.evaluate("el => el.click()")
                return
            except Exception as _js_e:
                _last_err = _js_e
            write_log_entry(
                batch_id, category,
                f"Рутьюб: Клик «Опубликовать» не прошёл (попытка {_attempt}/3).",
                level="warn",
            )
            page.wait_for_timeout(400)
    raise RutubeApiError(
        f"Не удалось нажать «Опубликовать» в Рутьюбе: {_last_err}"
    ) from _last_err

def _rutube_publish_button_ready(pub_btn) -> bool:
    try:
        if not pub_btn.is_visible(timeout=300):
            return False
        return not pub_btn.is_disabled(timeout=300)
    except Exception:
        return False

def _submit_rutube_publish(page, category, batch_id, pub_btn=None) -> None:
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
    _click_rutube_publish_button(pub_btn, page, category, batch_id)

def _click_rutube_add_button(add_btn, page, category, batch_id) -> None:
    """Клик «+ Добавить» с обходом перекрывающих оверлеев."""
    _last_err = None
    for _attempt in range(1, 6):
        try:
            page.keyboard.press("Escape")
            page.wait_for_timeout(300)
        except Exception:
            pass
        try:
            add_btn.scroll_into_view_if_needed(timeout=3_000)
        except Exception:
            pass
        try:
            add_btn.click(timeout=5_000)
            return
        except Exception as _e:
            _last_err = _e
            try:
                add_btn.click(force=True, timeout=3_000)
                return
            except Exception as _force_e:
                _last_err = _force_e
            try:
                add_btn.evaluate("el => el.click()")
                return
            except Exception as _js_e:
                _last_err = _js_e
            write_log_entry(
                batch_id, category,
                f"Рутьюб: Клик «+ Добавить» заблокирован (попытка {_attempt}/5).",
                level="warn",
            )
            page.wait_for_timeout(500)
    raise RutubeApiError(
        f"Не удалось нажать «+ Добавить» в студии Рутьюба: {_last_err}"
    ) from _last_err

def _publish_ui(page, video_path: str, category, batch_id=None):
    """Управляет браузером для публикации видео через UI Рутьюба."""

    # ── Шаг 1: Переходим в студию ────────────────────────────────────────
    write_log_entry(batch_id, category, "Рутьюб: Переход в студию Рутьюба.")
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
                f"Рутьюб: попытка {_attempt}/5 перейти в студию не удалась: {_e}",
                level="warn",
            )
            if _attempt < 5:
                _snap(page, batch_id)
    if _last_err is not None:
        raise RutubeApiError(
            f"Не удалось перейти в студию Рутьюба после 5 попыток: {_last_err}"
        ) from _last_err
    write_log_entry(
        batch_id, category,
        f"Рутьюб: domcontentloaded за {_time.monotonic() - _nav_started:.1f} с.",
    )
    _snap(page, batch_id)

    cur = page.url
    write_log_entry(batch_id, category, f"URL после перехода: {cur}", level='silent')
    from services.publish_auth_check import raise_if_login_required

    raise_if_login_required(page, "rutube")

    # ── Шаг 2: Кнопка «+ Добавить» ───────────────────────────────────────
    write_log_entry(batch_id, category, "Рутьюб: Ищу кнопку «+ Добавить».")
    add_btn = _wait_rutube_add_button(page, category, batch_id=batch_id)
    _click_rutube_add_button(add_btn, page, category, batch_id)
    write_log_entry(batch_id, category, "Рутьюб: Кнопка «+ Добавить» нажата, жду меню.")
    _snap(page, batch_id)

    # ── Шаг 3: «Загрузить видео или Shorts» из меню ──────────────────────
    write_log_entry(batch_id, category, "Рутьюб: Выбираю «Загрузить видео или Shorts».")
    upload_item = page.get_by_text("Загрузить видео или Shorts", exact=False).first
    try:
        upload_item.wait_for(state="visible", timeout=180_000)
    except Exception:
        write_log_entry(batch_id, category, "Рутьюб: exact-match не нашёл — пробую contains.")
        upload_item = page.get_by_text("Загрузить видео", exact=False).first
        upload_item.wait_for(state="visible", timeout=180_000)
    upload_item.click()
    write_log_entry(batch_id, category, "Рутьюб: «Загрузить видео или Shorts» нажато")
    _snap(page, batch_id)

    # ── Шаг 4: Нажимаем «Выбрать файлы» и передаём видео ────────────────
    write_log_entry(batch_id, category, "Рутьюб: Ищу поле загрузки файла.")
    choose_btn = page.get_by_text("Выбрать файлы", exact=False).first
    choose_btn.wait_for(state="visible", timeout=180_000)
    write_log_entry(batch_id, category, "Рутьюб: Кнопка «Выбрать файлы» найдена, открываю диалог выбора файла.")
    with page.expect_file_chooser(timeout=180_000) as fc_info:
        choose_btn.click()
    file_chooser = fc_info.value
    file_chooser.set_files(video_path)
    write_log_entry(batch_id, category, "Рутьюб: Файл передан браузеру, жду загрузки.")
    write_log_entry(batch_id, category, f"Файл: {os.path.basename(video_path)}", level='silent')
    _snap(page, batch_id)

    # ── Шаг 5: Ждём завершения загрузки ───────────────────────────────────
    _upload_ok = _wait_rutube_upload(page, category, batch_id=batch_id)
    _snap(page, batch_id)

    # ── Шаг 6: Выбираем категорию ─────────────────────────────────────────
    write_log_entry(batch_id, category, f"Рутьюб: Выбираю категорию «{_CATEGORY}».")
    _cat_ok = False
    try:
        cat_trigger = page.locator("text=Выберите категорию").first
        cat_trigger.wait_for(state="visible", timeout=5_000)
        cat_trigger.click()
        page.wait_for_timeout(500)
        page.keyboard.type(_CATEGORY)
        page.wait_for_timeout(600)
        cat_option = page.get_by_text(_CATEGORY, exact=True).first
        cat_option.wait_for(state="visible", timeout=5_000)
        cat_option.click()
        page.wait_for_timeout(500)
        _cat_ok = True
        write_log_entry(batch_id, category, f"Рутьюб: Категория «{_CATEGORY}» выбрана")
    except Exception as _e:
        write_log_entry(batch_id, category, "Рутьюб: Не удалось выбрать категорию — продолжаю.")
        write_log_entry(batch_id, category, f"Ошибка категории: {_e}", level='silent')
    _snap(page, batch_id)

    if not _cat_ok and not _upload_ok:
        raise RutubeApiError(
            "Рутьюб: загрузка не подтверждена и категория не выбрана — "
            "вероятно, сессия устарела или изменился интерфейс"
        )

    # ── Шаг 7: Нажимаем «Опубликовать» ───────────────────────────────────
    write_log_entry(batch_id, category, "Рутьюб: Прокручиваю к кнопке «Опубликовать».")
    _snap(page, batch_id)
    pub_btn = page.locator("button:has-text('Опубликовать')").last
    pub_btn.wait_for(state="visible", timeout=180_000)

    write_log_entry(batch_id, category, "Рутьюб: Нажимаю «Опубликовать».")
    _submit_rutube_publish(page, category, batch_id, pub_btn)
    _snap(page, batch_id)

    # ── Шаг 8: Проверяем успех (тост «Видео опубликовано») ──────────────
    write_log_entry(batch_id, category, "Рутьюб: Проверяю результат публикации.")

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
                "Рутьюб: Кнопка «Опубликовать» всё ещё видна — повторный клик "
                f"({_publish_retries}/{_PUBLISH_RETRY_MAX}).",
            )
            _submit_rutube_publish(page, category, batch_id)
            _snap(page, batch_id)

    _snap(page, batch_id)

    if _success:
        write_log_entry(
            batch_id, category,
            f"Рутьюб: Публикация успешна ({_success_via}).",
        )
        write_log_entry(batch_id, category, f"URL: {page.url}", level='silent')
    elif not _upload_ok:
        raise RutubeApiError(
            "Рутьюб: загрузка не подтверждена и публикация не подтверждена — "
            "вероятно, сессия устарела или изменился интерфейс"
        )
    else:
        write_log_entry(
            batch_id, category,
            "Рутьюб: Публикация не подтверждена после клика «Опубликовать».",
            level="warn",
        )
        write_log_entry(batch_id, category, f"URL: {page.url}", level='silent')
        raise RutubeApiError(
            "Рутьюб: публикация не подтверждена после клика «Опубликовать»"
        )
