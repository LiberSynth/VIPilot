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

from log import write_log_entry
from utils.utils import fmt_id_msg
from routes.api import build_publication_title, publication_file_name, hashtags


_NAV_TIMEOUT  = 30_000   # ms — таймаут навигации
_UPLOAD_WAIT  = 180_000  # ms — ожидание завершения загрузки (до 3 минут)


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
    log_id,
    batch_id=None,
    target_id: str | None = None,
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
        raise VkVideoSessionMissing("target_id не передан — невозможно загрузить сессию")

    session = db_get_target_session_context(target_id)
    if not session:
        raise VkVideoSessionMissing(
            "Браузерная сессия VK Видео не сохранена — "
            "авторизуйтесь в браузере (вкладка «Публикация»)"
        )

    saved_cookies = session.get("cookies", [])

    write_log_entry(log_id, fmt_id_msg("VK Видео: {} КБ, club_id={}", len(video_data) // 1024, club_id))

    pub_title = build_publication_title()
    file_name = publication_file_name(pub_title)
    tmp_dir = tempfile.mkdtemp()
    video_path = os.path.join(tmp_dir, file_name)
    try:
        with open(video_path, "wb") as _f:
            _f.write(video_data)

        _state = {"clip_url": ""}

        def _do_publish(page, _ctx):
            _state["clip_url"] = _publish_ui(page, club_id, video_path, pub_title, log_id, batch_id=batch_id)

        result = _get_browser("vkvideo").run_pipeline_browser(_do_publish, saved_cookies)

        if not result["ok"]:
            err = result.get("error", "Неизвестная ошибка")
            if "истекла" in err or "авторизуйтесь" in err:
                raise VkVideoCsrfExpired(err)
            raise VkVideoApiError(err)

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    write_log_entry(log_id, "VK Видео: клип опубликован успешно")
    return {"ok": True, "clip_url": _state["clip_url"], "pub_title": pub_title}


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
        write_log_entry(None, f"[vkvideo] _snap: {_e}", level='silent')


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


def _publish_ui(page, club_id: str, video_path: str, pub_title: str, log_id, batch_id=None):
    """Управляет браузером для публикации клипа через UI VK Видео."""

    cabinet_url = f"https://cabinet.vkvideo.ru/dashboard/@club{club_id}?showUploader=1&isClipUploading=1"

    # ── Шаг 1: Переходим в кабинет с параметром uploader ─────────────────
    write_log_entry(log_id, "VK Видео: Переход в кабинет автора…")
    page.goto(cabinet_url, wait_until="domcontentloaded", timeout=_NAV_TIMEOUT)
    page.wait_for_timeout(3000)
    _snap(page, batch_id)

    cur = page.url
    write_log_entry(log_id, f"VK Видео: URL после перехода: {cur}")
    if "vk.com/login" in cur or "/auth" in cur or "passport" in cur:
        raise VkVideoCsrfExpired(
            "Сессия истекла — авторизуйтесь снова в браузере (вкладка «Публикация»)"
        )

    # ── Шаг 2: Ждём появления кнопки «Выбрать файл» ──────────────────────
    write_log_entry(log_id, "VK Видео: Жду модал загрузки клипа…")
    choose_btn = page.locator("button:has-text('Выбрать файл')").first
    try:
        choose_btn.wait_for(state="visible", timeout=20_000)
    except Exception:
        write_log_entry(log_id, "VK Видео: «Выбрать файл» не появился — пробую альтернативный селектор…")
        choose_btn = page.get_by_text("Выбрать файл", exact=False).first
        choose_btn.wait_for(state="visible", timeout=10_000)

    write_log_entry(log_id, "VK Видео: Кнопка «Выбрать файл» найдена, загружаю файл…")

    # ── Шаг 3: Загружаем файл через file chooser ─────────────────────────
    with page.expect_file_chooser(timeout=15_000) as fc_info:
        choose_btn.click()
    file_chooser = fc_info.value
    file_chooser.set_files(video_path)
    write_log_entry(log_id, "VK Видео: Файл передан, жду форму «Публикация клипа»…")
    _snap(page, batch_id)

    # ── Шаг 4: Ждём форму «Публикация клипа» (поле Описание) ─────────────
    write_log_entry(log_id, "VK Видео: Жду форму публикации…")
    _form_ok = False
    try:
        page.wait_for_selector(
            "textarea[placeholder*='клип'], "
            "textarea[placeholder*='Клип'], "
            "[placeholder*='клип']",
            timeout=_UPLOAD_WAIT,
        )
        _form_ok = True
        write_log_entry(log_id, "VK Видео: Форма публикации открылась")
    except Exception:
        write_log_entry(log_id, "VK Видео: Ожидание формы истекло — продолжаю…")
        page.wait_for_timeout(5000)
    _snap(page, batch_id)

    # ── Шаг 5: Читаем ссылку на клип из DOM ──────────────────────────────
    clip_url = _read_clip_url(page)
    if clip_url:
        write_log_entry(log_id, f"VK Видео: Ссылка на клип: {clip_url}")
    else:
        write_log_entry(log_id, "VK Видео: Ссылка на клип не найдена — продолжаю…")

    # ── Шаг 6: Заполняем поле «Описание» хэштегами ────────────────────────
    write_log_entry(log_id, "VK Видео: Заполняю описание хэштегами…")
    try:
        desc_field = page.locator(
            "textarea[placeholder*='клип'], "
            "textarea[placeholder*='Клип'], "
            "[placeholder*='клип']"
        ).first
        desc_field.wait_for(state="visible", timeout=5_000)
        desc_field.click()
        description = f"{pub_title}. {hashtags()}"
        desc_field.fill(description)
        write_log_entry(log_id, f"VK Видео: Описание заполнено: {description}")
        _snap(page, batch_id)
    except Exception as _e:
        write_log_entry(log_id, "VK Видео: Не удалось заполнить описание — продолжаю…")

    # ── Шаг 7: Выключаем ненужные переключатели ──────────────────────────
    _TOGGLES_OFF = ["Показать на главной сообщества"]
    write_log_entry(log_id, "VK Видео: Выключаю переключатели…")
    for label_text in _TOGGLES_OFF:
        try:
            label = page.locator(f"label:has-text('{label_text}')").first
            if label.count() == 0 or not label.is_visible(timeout=3_000):
                continue
            chk = label.locator("input[type='checkbox']")
            if chk.count() > 0 and chk.first.is_checked():
                chk.first.click(force=True)
                page.wait_for_timeout(300)
                write_log_entry(log_id, f"VK Видео: «{label_text}» — выключено")
            else:
                toggle_btn = label.locator("[role='switch'], button").first
                if toggle_btn.count() > 0:
                    aria = toggle_btn.get_attribute("aria-checked")
                    if aria == "true":
                        toggle_btn.click()
                        page.wait_for_timeout(300)
                        write_log_entry(log_id, f"VK Видео: «{label_text}» — выключено")
                else:
                    label.click()
                    page.wait_for_timeout(300)
                    write_log_entry(log_id, f"VK Видео: «{label_text}» — кликнут")
        except Exception as _e:
            write_log_entry(log_id, f"VK Видео: Не удалось выключить «{label_text}» — продолжаю…")
    _snap(page, batch_id)

    # ── Шаг 8: Ждём доступную кнопку «Опубликовать» ──────────────────────
    write_log_entry(log_id, "VK Видео: Жду кнопку «Опубликовать»…")
    pub_btn = page.locator("button:has-text('Опубликовать')").last
    pub_btn.wait_for(state="visible", timeout=90_000)
    _snap(page, batch_id)

    # ── Шаг 9: Нажимаем «Опубликовать» ───────────────────────────────────
    write_log_entry(log_id, "VK Видео: Нажимаю «Опубликовать»…")
    pub_btn.click()
    page.wait_for_timeout(3000)
    _snap(page, batch_id)

    # ── Шаг 10: Проверяем успех (тост «Клип опубликован») ────────────────
    write_log_entry(log_id, "VK Видео: Проверяю результат публикации…")

    _SUCCESS_TEXTS = ["Клип опубликован", "опубликован в канале"]
    _ERROR_TEXTS = [
        "Ошибка публикации",
        "не удалось опубликовать",
        "Произошла ошибка",
        "Клип не опубликован",
    ]

    _deadline = _time.monotonic() + 15
    _success = False
    while _time.monotonic() < _deadline:
        try:
            body = page.locator("body").inner_text(timeout=1500)
            for err in _ERROR_TEXTS:
                if err.lower() in body.lower():
                    raise VkVideoApiError(
                        f"VK Видео заблокировал публикацию: «{err}»."
                    )
            for ok_text in _SUCCESS_TEXTS:
                if ok_text.lower() in body.lower():
                    _success = True
                    break
        except VkVideoApiError:
            raise
        except Exception:
            pass
        if _success:
            break
        page.wait_for_timeout(1000)

    _snap(page, batch_id)

    if _success:
        write_log_entry(log_id, "VK Видео: Клип опубликован успешно.")
    else:
        write_log_entry(log_id, "VK Видео: Публикация завершена (тост не обнаружен, ошибок нет)")

    if not _form_ok and not _success:
        raise VkVideoApiError(
            "VK Видео: форма публикации не открылась и тост успеха не найден — "
            "вероятно, сессия устарела или изменился интерфейс"
        )

    return clip_url
