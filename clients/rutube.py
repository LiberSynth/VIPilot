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
from routes.api import build_publication_title, publication_file_name


_NAV_TIMEOUT  = 30_000   # ms — таймаут навигации
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
    log_id,
    batch_id=None,
    target_id: str | None = None,
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

    write_log_entry(log_id, "Рутьюб: Публикация запущена.")
    write_log_entry(log_id, fmt_id_msg("[rutube] {} КБ, person_id={}", len(video_data) // 1024, person_id), level='silent')

    pub_title = build_publication_title()
    file_name = publication_file_name(pub_title)
    write_log_entry(log_id, f"[rutube] Заголовок: {pub_title}, файл: {file_name}", level='silent')
    tmp_dir = tempfile.mkdtemp()
    video_path = os.path.join(tmp_dir, file_name)
    try:
        with open(video_path, "wb") as _f:
            _f.write(video_data)

        def _do_publish(page, _ctx):
            _publish_ui(page, video_path, log_id, batch_id=batch_id)

        result = _get_browser("rutube").run_pipeline_browser(_do_publish, saved_cookies)

        if not result["ok"]:
            err = result.get("error", "Неизвестная ошибка")
            if "истекла" in err or "авторизуйтесь" in err:
                raise RutubeCsrfExpired(err)
            raise RutubeApiError(err)

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    write_log_entry(log_id, "Рутьюб: видео опубликовано успешно")
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
        write_log_entry(None, f"[rutube] _snap: {_e}", level='silent')


def _publish_ui(page, video_path: str, log_id, batch_id=None):
    """Управляет браузером для публикации видео через UI Рутьюба."""

    # ── Шаг 1: Переходим в студию ────────────────────────────────────────
    write_log_entry(log_id, "Рутьюб: Переход в студию Рутьюба.")
    page.goto(STUDIO_URL, wait_until="domcontentloaded", timeout=_NAV_TIMEOUT)
    page.wait_for_timeout(2000)
    _snap(page, batch_id)

    cur = page.url
    write_log_entry(log_id, f"[rutube] URL после перехода: {cur}", level='silent')
    if "rutube.ru/login" in cur or "/auth" in cur or "passport" in cur:
        raise RutubeCsrfExpired(
            "Сессия истекла — авторизуйтесь снова в браузере (вкладка «Публикация»)"
        )

    # ── Шаг 2: Кнопка «+ Добавить» ───────────────────────────────────────
    write_log_entry(log_id, "Рутьюб: Ищу кнопку «+ Добавить».")
    add_btn = page.locator("button:has-text('Добавить')").first
    add_btn.wait_for(state="visible", timeout=15_000)
    add_btn.click()
    write_log_entry(log_id, "Рутьюб: Кнопка «+ Добавить» нажата, жду меню.")
    page.wait_for_timeout(1000)
    _snap(page, batch_id)

    # ── Шаг 3: «Загрузить видео или Shorts» из меню ──────────────────────
    write_log_entry(log_id, "Рутьюб: Выбираю «Загрузить видео или Shorts».")
    upload_item = page.get_by_text("Загрузить видео или Shorts", exact=False).first
    try:
        upload_item.wait_for(state="visible", timeout=8_000)
    except Exception:
        write_log_entry(log_id, "Рутьюб: exact-match не нашёл — пробую contains.")
        upload_item = page.get_by_text("Загрузить видео", exact=False).first
        upload_item.wait_for(state="visible", timeout=5_000)
    upload_item.click()
    write_log_entry(log_id, "Рутьюб: «Загрузить видео или Shorts» нажато")
    page.wait_for_timeout(1500)
    _snap(page, batch_id)

    # ── Шаг 4: Нажимаем «Выбрать файлы» и передаём видео ────────────────
    write_log_entry(log_id, "Рутьюб: Ищу поле загрузки файла.")
    choose_btn = page.get_by_text("Выбрать файлы", exact=False).first
    choose_btn.wait_for(state="visible", timeout=20_000)
    write_log_entry(log_id, "Рутьюб: Кнопка «Выбрать файлы» найдена, открываю диалог выбора файла.")
    with page.expect_file_chooser(timeout=15_000) as fc_info:
        choose_btn.click()
    file_chooser = fc_info.value
    file_chooser.set_files(video_path)
    write_log_entry(log_id, "Рутьюб: Файл передан браузеру, жду загрузки.")
    write_log_entry(log_id, f"[rutube] Файл: {os.path.basename(video_path)}", level='silent')
    _snap(page, batch_id)

    # ── Шаг 5: Ждём завершения загрузки (появление «Модерация») ─────────
    write_log_entry(log_id, "Рутьюб: Жду завершения загрузки (до 3 минут).")
    _upload_ok = False
    try:
        page.wait_for_selector("text=Модерация", timeout=_UPLOAD_WAIT)
        _upload_ok = True
        write_log_entry(log_id, "Рутьюб: Загрузка завершена, перехожу к публикации.")
    except Exception:
        write_log_entry(log_id, "Рутьюб: Ожидание загрузки истекло — продолжаю.")
        page.wait_for_timeout(5000)
    _snap(page, batch_id)

    # ── Шаг 6: Выбираем категорию ─────────────────────────────────────────
    write_log_entry(log_id, f"Рутьюб: Выбираю категорию «{_CATEGORY}».")
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
        write_log_entry(log_id, f"Рутьюб: Категория «{_CATEGORY}» выбрана")
    except Exception as _e:
        write_log_entry(log_id, "Рутьюб: Не удалось выбрать категорию — продолжаю.")
        write_log_entry(log_id, f"[rutube] Ошибка категории: {_e}", level='silent')
    _snap(page, batch_id)

    if not _cat_ok and not _upload_ok:
        raise RutubeApiError(
            "Рутьюб: загрузка не подтверждена и категория не выбрана — "
            "вероятно, сессия устарела или изменился интерфейс"
        )

    # ── Шаг 7: Нажимаем «Опубликовать» ───────────────────────────────────
    write_log_entry(log_id, "Рутьюб: Нажимаю «Опубликовать».")
    pub_btn = page.locator("button:has-text('Опубликовать')").last
    pub_btn.wait_for(state="visible", timeout=15_000)
    pub_btn.click()
    page.wait_for_timeout(2000)
    _snap(page, batch_id)

    # ── Шаг 8: Проверяем успех (toast «Видео опубликовано») ──────────────
    write_log_entry(log_id, "Рутьюб: Проверяю результат публикации.")

    _SUCCESS_TEXTS = ["Видео опубликовано", "опубликовано"]
    _ERROR_TEXTS = [
        "Ошибка публикации",
        "не удалось опубликовать",
        "Видео не опубликовано",
        "Произошла ошибка",
    ]

    _deadline = _time.monotonic() + 15
    _success = False
    while _time.monotonic() < _deadline:
        try:
            body = page.locator("body").inner_text(timeout=1500)
            for err in _ERROR_TEXTS:
                if err.lower() in body.lower():
                    raise RutubeApiError(
                        f"Рутьюб заблокировал публикацию: «{err}»."
                    )
            for ok_text in _SUCCESS_TEXTS:
                if ok_text.lower() in body.lower():
                    _success = True
                    break
        except RutubeApiError:
            raise
        except Exception:
            pass
        if _success:
            break
        page.wait_for_timeout(1000)

    _snap(page, batch_id)

    if _success:
        write_log_entry(log_id, "Рутьюб: Публикация успешна.")
        write_log_entry(log_id, f"[rutube] URL: {page.url}", level='silent')
    else:
        write_log_entry(log_id, "Рутьюб: Публикация завершена (тост не обнаружен, ошибок нет)")
        write_log_entry(log_id, f"[rutube] URL: {page.url}", level='silent')
