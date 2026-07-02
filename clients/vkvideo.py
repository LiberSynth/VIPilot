"""
VK Видео клиент: публикует клип через веб-интерфейс кабинета автора (UI-driven).

Playwright управляет браузером, скриншоты стримятся в виджет «Публикация».
Видео загружается через expect_file_chooser() — как обычный пользователь.

Поток:
  1. Открыть кабинет с параметром showUploader
  2. Выбрать файл через file chooser
  3. Прочитать ссылку на клип из DOM
  4. Заполнить описание хэштегами
  5. Дождаться обложек → нажать «Опубликовать»
  6. Дождаться тоста «Клип опубликован»
"""

import os
import shutil
import tempfile
import time as _time

from clients.common import dismiss_overlays, raise_if_login_required, safe_click
from db import db_set_batch_vkvideo_clip_url
from log import write_log_entry
from utils.utils import fmt_id_msg
from routes.api import publication_file_name, hashtags

_NAV_TIMEOUT = 60_000  # ms — таймаут одной попытки навигации (1 минута; до 5 попыток подряд)
_UPLOAD_WAIT = 180_000  # ms — ожидание завершения загрузки (до 3 минут)
_PUBLISH_WAIT = 300_000  # ms — готовность превью/кнопки и подтверждение (до 5 минут)

class VkVideoSessionMissing(RuntimeError):
    """Браузерная сессия VK Видео не сохранена — требуется авторизация."""

class VkVideoCsrfExpired(RuntimeError):
    """Сессия истекла — необходима повторная авторизация."""

class VkVideoApiError(RuntimeError):
    """Ошибка публикации на VK Видео."""

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
    Публикует клип на VK Видео через веб-интерфейс кабинета автора.
    Браузер виден в панели «Публикация» — можно наблюдать весь процесс.
    Возвращает True при успехе.
    """
    from services.browser_registry import get_browser as _get_browser
    from db import db_get_target_session_context

    cfg = target_config or {}
    club_id = cfg.get("club_id", "")

    if not club_id:
        raise VkVideoApiError("club_id не задан в настройках VK Видео")

    if not target_id:
        raise VkVideoSessionMissing(
            "target_id не передан — невозможно загрузить сессию"
        )

    session = db_get_target_session_context(target_id)
    if not session:
        raise VkVideoSessionMissing(
            "Браузерная сессия VK Видео не сохранена — "
            "авторизуйтесь в браузере (вкладка «Публикация»)"
        )

    saved_cookies = session.get("cookies", [])

    write_log_entry(batch_id, category, "VK Видео: Публикация запущена.")
    write_log_entry(
        batch_id, category,
        fmt_id_msg("[vkvideo] {} КБ, club_id={}", len(video_data) // 1024, club_id),
        level="silent",
    )

    file_name = publication_file_name(pub_title)
    write_log_entry(
        batch_id, category, f"Заголовок: {pub_title}, файл: {file_name}", level="silent"
    )
    tmp_dir = tempfile.mkdtemp()
    video_path = os.path.join(tmp_dir, file_name)
    try:
        with open(video_path, "wb") as _f:
            _f.write(video_data)

        _state = {"clip_url": ""}

        def _do_publish(page, _ctx):
            _state["clip_url"] = _publish_ui(
                page, club_id, video_path, pub_title, category, batch_id=batch_id
            )
            if _state["clip_url"] and batch_id:
                db_set_batch_vkvideo_clip_url(batch_id, _state["clip_url"])

        result = _get_browser("vkvideo").run_pipeline_browser(
            _do_publish, saved_cookies, batch_id=batch_id, category=category,
            batch_session=batch_session, keep_browser=keep_browser,
        )

        if not result["ok"]:
            err = result.get("error", "Неизвестная ошибка")
            if "истекла" in err or "авторизуйтесь" in err:
                raise VkVideoCsrfExpired(err)
            raise VkVideoApiError(err)

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        if not keep_browser and batch_session is None:
            try:
                _get_browser("vkvideo").stop(batch_id=batch_id, category=category)
            except Exception:
                pass

    return {"ok": True, "clip_url": _state["clip_url"]}

# ---------------------------------------------------------------------------
# UI-driven публикация
# ---------------------------------------------------------------------------

def _snap(page, batch_id=None) -> None:
    """Снимает скриншот и передаёт кадр в SSE-трансляцию и монитор (thread-safe)."""
    try:
        from services.browser_registry import get_browser as _get_browser

        _b = _get_browser("vkvideo")
        img = page.screenshot(type="jpeg", quality=65)
        _b.push_frame(img)
        if batch_id:
            _b.push_frame_for_batch(batch_id, img)
    except Exception as _e:
        write_log_entry(None, 'vkvideo', f'_snap: {_e}', level='silent')

def _read_clip_url(page) -> str:
    """Читает ссылку на клип из DOM (раздел «Ссылка на клип»)."""
    try:
        link = page.locator("a[href*='/clip-']").first
        href = link.get_attribute("href", timeout=5_000)
        if href:
            return href if href.startswith("http") else f"https://vkvideo.ru{href}"
    except Exception:
        pass
    try:
        body = page.locator("body").inner_text(timeout=3_000)
        for part in body.split():
            if "vkvideo.ru/clip-" in part:
                return part.strip()
    except Exception:
        pass
    return ""

_VK_PUBLISH_SUCCESS_TEXTS = ("Клип опубликован", "опубликован в канале")
_VK_PUBLISH_ERROR_TEXTS = (
    "Ошибка публикации",
    "не удалось опубликовать",
    "Произошла ошибка",
    "Клип не опубликован",
)

_VK_PUBLISH_FORM_SELECTORS = (
    "textarea[placeholder*='клип']",
    "textarea[placeholder*='Клип']",
    "[placeholder*='клип']",
    "[contenteditable='true']",
    "[role='textbox']",
)

def _vk_publish_form_visible(page) -> bool:
    for _sel in _VK_PUBLISH_FORM_SELECTORS:
        try:
            if page.locator(_sel).first.is_visible(timeout=200):
                return True
        except Exception:
            continue
    return False

def _vk_publish_modal_visible(page) -> bool:
    """Модалка «Публикация клипа» открыта (поле описания или заголовок модала)."""
    if _vk_publish_form_visible(page):
        return True
    for _text in ("Публикация клипа", "Ссылка на клип", "Название файла"):
        try:
            if page.get_by_text(_text, exact=False).first.is_visible(timeout=200):
                return True
        except Exception:
            continue
    return False

def _detect_vk_captcha(page) -> bool:
    try:
        return page.get_by_text("Продолжить", exact=False).first.is_visible(timeout=200)
    except Exception:
        return False

def _handle_vk_captcha(page, category, batch_id) -> None:
    try:
        cont_btn = page.get_by_text("Продолжить", exact=False).first
        if cont_btn.is_visible(timeout=300):
            cont_btn.click()
            write_log_entry(
                batch_id, category,
                "VK Видео: CAPTCHA-диалог закрыт («Продолжить» нажато).",
            )
            _snap(page, batch_id)
    except Exception:
        pass

VKVIDEO_PUBLISH_WHITELIST = [
    ("captcha", _detect_vk_captcha, _handle_vk_captcha),
    ("publish_modal", _vk_publish_modal_visible, None),
]

def _vkvideo_dismiss(page, category, batch_id) -> None:
    dismiss_overlays(page, VKVIDEO_PUBLISH_WHITELIST, batch_id, category, label="VK Видео")

def _vk_publish_button_visible(page) -> bool:
    try:
        return page.locator("button:has-text('Опубликовать')").last.is_visible(timeout=300)
    except Exception:
        return False

_VK_PREVIEW_IMG_SELECTORS = (
    "img[src*='userapi']",
    "img[src*='vkuserphoto']",
    "img[src*='mycdn']",
    "img[src*='okcdn']",
    "img[src^='blob:']",
)

def _vk_clip_preview_ready(page) -> bool:
    """Превью клипа обработано: в панели есть кадр видео или обложка."""
    try:
        vid = page.locator("video").first
        if vid.is_visible(timeout=200):
            if vid.evaluate(
                "el => el.readyState >= 2 || !!(el.poster && el.poster.length)"
            ):
                return True
    except Exception:
        pass
    for _sel in _VK_PREVIEW_IMG_SELECTORS:
        try:
            imgs = page.locator(_sel)
            for _i in range(min(imgs.count(), 8)):
                img = imgs.nth(_i)
                if not img.is_visible(timeout=100):
                    continue
                box = img.bounding_box()
                if box and box.get("width", 0) >= 48 and box.get("height", 0) >= 48:
                    return True
        except Exception:
            continue
    return False

def _scroll_vk_publish_button(pub_btn) -> None:
    try:
        pub_btn.scroll_into_view_if_needed(timeout=3_000)
    except Exception:
        pass

def _vk_publish_button_clickable(pub_btn) -> bool:
    """Кнопка «Опубликовать» реально доступна (не серая/disabled)."""
    try:
        if not pub_btn.is_visible(timeout=300):
            return False
        return pub_btn.evaluate("""el => {
            if (el.disabled) return false;
            if (el.getAttribute('aria-disabled') === 'true') return false;
            const st = window.getComputedStyle(el);
            if (st.pointerEvents === 'none') return false;
            if (parseFloat(st.opacity) < 0.55) return false;
            if (st.visibility === 'hidden' || st.display === 'none') return false;
            const r = el.getBoundingClientRect();
            if (r.width < 8 || r.height < 8) return false;
            return true;
        }""")
    except Exception:
        return False

def _wait_vk_clip_publish_ready(page, pub_btn, batch_id, category, timeout_ms=_PUBLISH_WAIT):
    """Ждёт, пока кнопка «Опубликовать» станет доступной (enabled)."""
    write_log_entry(
        batch_id, category,
        "VK Видео: Жду доступности кнопки «Опубликовать» (обработка видео).",
    )
    deadline = _time.monotonic() + timeout_ms / 1000
    _next_snap = _time.monotonic()
    while _time.monotonic() < deadline:
        raise_if_login_required(page, "vkvideo")
        _vkvideo_dismiss(page, category, batch_id)
        _scroll_vk_publish_button(pub_btn)
        if _vk_publish_button_clickable(pub_btn):
            write_log_entry(batch_id, category, "VK Видео: Кнопка «Опубликовать» доступна.")
            return
        if _time.monotonic() >= _next_snap:
            _snap(page, batch_id)
            _next_snap = _time.monotonic() + 2
        page.wait_for_timeout(1_000)
    raise VkVideoApiError(
        "VK Видео: таймаут ожидания доступности кнопки «Опубликовать»"
    )

def _vk_publish_confirmed_after_submit(page) -> bool:
    """Признак успеха после клика «Опубликовать»: модалка и кнопка submit исчезли."""
    if _vk_publish_modal_visible(page) or _vk_publish_button_visible(page):
        return False
    try:
        url = page.url.lower()
    except Exception:
        return False
    if "video_my_content_clips" in url:
        return True
    return (
        "cabinet.vkvideo.ru/dashboard" in url
        and "showuploader" not in url
    )

def _check_vk_publish_result(page, *, after_submit: bool = False) -> tuple[bool, str | None]:
    """(True, причина) — публикация клипа подтверждена."""
    try:
        body = page.locator("body").inner_text(timeout=800)
        body_lower = body.lower()
        for err in _VK_PUBLISH_ERROR_TEXTS:
            if err.lower() in body_lower:
                raise VkVideoApiError(f"VK Видео заблокировал публикацию: «{err}».")
        for ok_text in _VK_PUBLISH_SUCCESS_TEXTS:
            if ok_text.lower() in body_lower:
                return True, "тост"
    except VkVideoApiError:
        raise
    except Exception:
        pass
    if after_submit and _vk_publish_confirmed_after_submit(page):
        return True, "URL/форма"
    return False, None

def _click_vk_publish_button(pub_btn, page, category, batch_id) -> None:
    """Клик по доступной кнопке «Опубликовать» без ожидания навигации после submit."""
    if not _vk_publish_button_clickable(pub_btn):
        raise VkVideoApiError(
            "VK Видео: кнопка «Опубликовать» недоступна — клик пропущен"
        )
    try:
        safe_click(
            pub_btn, page, VKVIDEO_PUBLISH_WHITELIST,
            batch_id=batch_id, category=category, label="VK Видео",
            timeout_ms=5_000, max_attempts=3,
            click_kwargs={"no_wait_after": True},
            js_fallback=True,
        )
    except Exception as _e:
        raise VkVideoApiError(
            f"Не удалось нажать «Опубликовать» в VK Видео: {_e}"
        ) from _e

def _submit_vk_clip_publish(page, category, batch_id, pub_btn=None) -> None:
    """Прокручивает к доступной кнопке и нажимает «Опубликовать»."""
    if pub_btn is None:
        pub_btn = page.locator("button:has-text('Опубликовать')").last
    _scroll_vk_publish_button(pub_btn)
    page.wait_for_timeout(300)
    if not _vk_publish_button_clickable(pub_btn):
        raise VkVideoApiError(
            "VK Видео: кнопка «Опубликовать» недоступна перед кликом"
        )
    _click_vk_publish_button(pub_btn, page, category, batch_id)

def _wait_visible(locator, timeout_ms: int, page, batch_id, interval_ms: int = 2_000):
    """Ждёт видимости локатора, снимая скриншот каждые interval_ms мс.
    Playwright sync API нельзя вызывать из других потоков — поэтому
    скриншоты делаем прямо здесь, в главном потоке Playwright."""
    deadline = _time.monotonic() + timeout_ms / 1000
    while True:
        raise_if_login_required(page, "vkvideo")
        _vkvideo_dismiss(page, None, batch_id)
        remaining = deadline - _time.monotonic()
        if remaining <= 0:
            raise_if_login_required(page, "vkvideo")
            locator.wait_for(state="visible", timeout=1_000)  # бросит TimeoutError
            return
        poll = min(interval_ms, max(500, int(remaining * 1000)))
        try:
            locator.wait_for(state="visible", timeout=poll)
            return  # виден
        except Exception:
            if _time.monotonic() >= deadline:
                locator.wait_for(state="visible", timeout=1_000)  # бросит TimeoutError
                return
            _snap(page, batch_id)

def _publish_ui(
    page, club_id: str, video_path: str, pub_title: str, category, batch_id=None
):
    """Управляет браузером для публикации клипа через UI VK Видео."""

    cabinet_url = f"https://cabinet.vkvideo.ru/dashboard/@club{club_id}?showUploader=1&isClipUploading=1"

    # ── Шаг 1: Переходим в кабинет с параметром uploader ─────────────────
    write_log_entry(batch_id, category, "VK Видео: Переход в кабинет автора.")
    _snap(page, batch_id)
    _last_err = None
    for _attempt in range(1, 6):
        try:
            page.goto(cabinet_url, wait_until="domcontentloaded", timeout=_NAV_TIMEOUT)
            _last_err = None
            break
        except Exception as _e:
            _last_err = _e
            write_log_entry(
                batch_id, category,
                f"VK Видео: попытка {_attempt}/5 перейти в кабинет не удалась: {_e}",
                level="warn",
            )
            if _attempt < 5:
                _snap(page, batch_id)
    if _last_err is not None:
        raise VkVideoApiError(
            f"Не удалось перейти в кабинет VK Видео после 5 попыток: {_last_err}"
        ) from _last_err
    _snap(page, batch_id)

    cur = page.url
    write_log_entry(batch_id, category, f"URL после перехода: {cur}", level="silent")

    raise_if_login_required(page, "vkvideo", club_id=club_id)

    # ── Шаг 1б: CAPTCHA «Проверяем, что вы не робот» или готовность модала ─
    choose_btn = page.get_by_text("Выбрать файл", exact=False).first
    _captcha_deadline = _time.monotonic() + 5
    while _time.monotonic() < _captcha_deadline:
        raise_if_login_required(page, "vkvideo", club_id=club_id)
        try:
            if choose_btn.is_visible(timeout=300):
                break
        except Exception:
            pass
        _vkvideo_dismiss(page, category, batch_id)
        page.wait_for_timeout(300)

    # ── Шаг 2: Ждём появления кнопки «Выбрать файл» ──────────────────────
    write_log_entry(batch_id, category, "VK Видео: Жду модал загрузки клипа.")
    _snap(page, batch_id)
    _wait_visible(choose_btn, 180_000, page, batch_id)
    _snap(page, batch_id)

    write_log_entry(batch_id, category, "VK Видео: Кнопка «Выбрать файл» найдена, загружаю файл.")

    # ── Шаг 3: Загружаем файл через file chooser ─────────────────────────
    _snap(page, batch_id)
    with page.expect_file_chooser(timeout=180_000) as fc_info:
        choose_btn.click()
    file_chooser = fc_info.value
    file_chooser.set_files(video_path)
    write_log_entry(batch_id, category, "VK Видео: Файл передан, жду форму «Публикация клипа».")
    write_log_entry(
        batch_id, category, f"Файл: {os.path.basename(video_path)}", level="silent"
    )
    _snap(page, batch_id)

    # ── Шаг 4: Ждём форму «Публикация клипа» (поле Описание) ─────────────
    write_log_entry(batch_id, category, "VK Видео: Жду форму публикации.")
    _form_ok = False
    _snap(page, batch_id)
    try:
        page.wait_for_selector(
            "textarea[placeholder*='клип'], "
            "textarea[placeholder*='Клип'], "
            "[placeholder*='клип']",
            timeout=_UPLOAD_WAIT,
        )
        _form_ok = True
        write_log_entry(batch_id, category, "VK Видео: Форма публикации открылась")
    except Exception:
        write_log_entry(batch_id, category, "VK Видео: Ожидание формы истекло — продолжаю.")
        page.wait_for_timeout(5000)
    _snap(page, batch_id)

    # ── Шаг 5: Читаем ссылку на клип из DOM ──────────────────────────────
    _snap(page, batch_id)
    clip_url = _read_clip_url(page)
    _snap(page, batch_id)
    if clip_url:
        write_log_entry(batch_id, category, "VK Видео: Ссылка на клип получена.")
        write_log_entry(batch_id, category, f"Ссылка на клип: {clip_url}", level="silent")
    else:
        write_log_entry(batch_id, category, "VK Видео: Ссылка на клип не найдена — продолжаю.")

    # ── Шаг 6: Заполняем поле «Описание» хэштегами ────────────────────────
    write_log_entry(batch_id, category, "VK Видео: Заполняю описание хэштегами.")
    write_log_entry(batch_id, category, f"Хэштеги: {hashtags()}", level="silent")
    try:
        desc_field = page.locator(
            "textarea[placeholder*='клип'], "
            "textarea[placeholder*='Клип'], "
            "[placeholder*='клип']"
        ).first
        _snap(page, batch_id)
        desc_field.wait_for(state="visible", timeout=5_000)
        _snap(page, batch_id)
        desc_field.click()
        description = f"{pub_title}. {hashtags()}"
        desc_field.fill(description)
        write_log_entry(batch_id, category, "VK Видео: Описание заполнено.")
        write_log_entry(batch_id, category, f"Описание: {description}", level="silent")
        _snap(page, batch_id)
    except Exception as _e:
        write_log_entry(batch_id, category, "VK Видео: Не удалось заполнить описание — продолжаю.")
        write_log_entry(batch_id, category, f"Ошибка описания: {_e}", level="silent")

    # ── Шаг 7: Ждём кнопку и готовность превью ───────────────────────────
    write_log_entry(batch_id, category, "VK Видео: Жду кнопку «Опубликовать».")
    pub_btn = page.locator("button:has-text('Опубликовать')").last
    _snap(page, batch_id)
    pub_btn.wait_for(state="visible", timeout=_PUBLISH_WAIT)
    _snap(page, batch_id)
    _wait_vk_clip_publish_ready(page, pub_btn, batch_id, category)

    # ── Шаг 8: Нажимаем «Опубликовать» ───────────────────────────────────
    # Кнопка внизу модалки — часто за пределами viewport; без прокрутки
    # Playwright висит на click() до таймаута, ожидая actionability.
    write_log_entry(batch_id, category, "VK Видео: Прокручиваю к кнопке «Опубликовать».")
    _snap(page, batch_id)

    write_log_entry(batch_id, category, "VK Видео: Нажимаю «Опубликовать».")
    _submit_vk_clip_publish(page, category, batch_id, pub_btn)
    _snap(page, batch_id)

    # ── Шаг 9: Проверяем успех (тост «Клип опубликован») ────────────────
    write_log_entry(batch_id, category, "VK Видео: Проверяю результат публикации.")

    _deadline = _time.monotonic() + _PUBLISH_WAIT / 1000
    _last_click_at = _time.monotonic()
    _success, _success_via = _check_vk_publish_result(page, after_submit=True)
    while _time.monotonic() < _deadline and not _success:
        page.wait_for_timeout(1_000)
        _success, _success_via = _check_vk_publish_result(page, after_submit=True)
        if _success:
            break
        _pub_btn = page.locator("button:has-text('Опубликовать')").last
        _scroll_vk_publish_button(_pub_btn)
        if (
            _vk_publish_button_clickable(_pub_btn)
            and _time.monotonic() - _last_click_at >= 1.0
        ):
            _last_click_at = _time.monotonic()
            _submit_vk_clip_publish(page, category, batch_id, _pub_btn)
            _snap(page, batch_id)

    _snap(page, batch_id)

    if _success:
        write_log_entry(
            batch_id, category,
            f"VK Видео: Клип опубликован успешно ({_success_via}).",
        )
        write_log_entry(batch_id, category, f"URL: {page.url}", level="silent")
    elif not _form_ok:
        raise VkVideoApiError(
            "VK Видео: форма публикации не открылась и успех не подтверждён — "
            "вероятно, сессия устарела или изменился интерфейс"
        )
    else:
        _form_vis = _vk_publish_form_visible(page)
        _modal_vis = _vk_publish_modal_visible(page)
        _btn_vis = _vk_publish_button_visible(page)
        _preview_vis = _vk_clip_preview_ready(page)
        _btn_clickable = False
        if _btn_vis:
            _btn_clickable = _vk_publish_button_clickable(
                page.locator("button:has-text('Опубликовать')").last
            )
        write_log_entry(
            batch_id, category,
            "VK Видео: Публикация не подтверждена после клика «Опубликовать». "
            f"URL={page.url}, form={_form_vis}, modal={_modal_vis}, "
            f"preview={_preview_vis}, button={_btn_vis}, clickable={_btn_clickable}",
            level="warn",
        )
        raise VkVideoApiError(
            "VK Видео: публикация не подтверждена после клика «Опубликовать»"
        )

    return clip_url
