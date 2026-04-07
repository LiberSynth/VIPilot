"""
Дзен-клиент: публикует короткое видео через веб-интерфейс Дзена (UI-driven).

Playwright управляет браузером, скриншоты стримятся в виджет «Публикация».
Видео загружается через set_input_files() — как обычный пользователь.
"""

import json
import os
import tempfile

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

def is_configured(cfg: dict) -> bool:
    """True если publisher_id задан и сессия сохранена."""
    from services.dzen_browser import profile_exists
    return bool(cfg.get("publisher_id")) and profile_exists()


# ---------------------------------------------------------------------------
# Публичный API
# ---------------------------------------------------------------------------

def publish(
    video_data: bytes,
    target_config: dict,
    title: str,
    log_id,
) -> bool:
    """
    Публикует видео на Дзен через веб-интерфейс.
    Браузер виден в панели «Публикация» — можно наблюдать весь процесс.
    Возвращает True при успехе.
    """
    from services.dzen_browser import DZEN_COOKIES_FILE, profile_exists, run_pipeline_browser

    cfg = target_config or {}
    publisher_id = cfg.get("publisher_id", "")

    if not publisher_id:
        raise DzenApiError("publisher_id не задан в настройках Дзен")

    if not profile_exists():
        raise DzenSessionMissing(
            "Браузерная сессия Дзен не сохранена — "
            "авторизуйтесь в браузере (вкладка «Публикация»)"
        )

    try:
        with open(DZEN_COOKIES_FILE, "r", encoding="utf-8") as _f:
            saved_cookies = json.load(_f)
    except Exception as e:
        raise DzenSessionMissing(f"Не удалось прочитать куки сессии: {e}")

    if log_id:
        db_log_entry(log_id, f"Дзен: {len(video_data) // 1024} КБ, publisher={publisher_id[:12]}…")

    # Пишем видео во временный файл (set_input_files требует путь)
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4")
    try:
        tmp.write(video_data)
        tmp.flush()
        tmp.close()
        video_path = tmp.name

        def _do_publish(page, ctx):
            _publish_ui(page, publisher_id, video_path, title, log_id)

        result = run_pipeline_browser(_do_publish, saved_cookies)

        if not result["ok"]:
            err = result.get("error", "Неизвестная ошибка")
            if "истекла" in err or "авторизуйтесь" in err:
                raise DzenCsrfExpired(err)
            raise DzenApiError(err)

    finally:
        try:
            os.unlink(tmp.name)
        except Exception:
            pass

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


def _snap(page) -> None:
    """Снимает скриншот и передаёт кадр в SSE-трансляцию (thread-safe)."""
    try:
        from services.dzen_browser import push_frame
        img = page.screenshot(type="jpeg", quality=65)
        push_frame(img)
    except Exception as _e:
        print(f"[dzen] _snap: {_e}")


def _publish_ui(page, publisher_id: str, video_path: str, title: str, log_id):
    """Управляет браузером для публикации видео через UI Дзена."""

    studio_url = f"https://dzen.ru/profile/editor/id/{publisher_id}/"

    # ── Шаг 1: Переходим в студию ────────────────────────────────────────
    _log(log_id, f"Переход в студию: {studio_url}")
    page.goto(studio_url, wait_until="domcontentloaded", timeout=_NAV_TIMEOUT)
    page.wait_for_timeout(2000)
    _snap(page)

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
            _snap(page)
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
    _snap(page)

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
    _snap(page)

    # ── Шаг 4: Загружаем файл ────────────────────────────────────────────
    _log(log_id, "Ищу поле загрузки файла…")
    # file input скрыт намеренно — ждём только "attached", не "visible"
    file_input = page.locator('input[type="file"]').first
    file_input.wait_for(state="attached", timeout=15_000)
    file_input.set_input_files(video_path)
    _log(log_id, "Файл передан браузеру, жду загрузки…")
    _snap(page)

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
    _snap(page)

    # ── Шаг 5: Вводим заголовок ──────────────────────────────────────────
    _log(log_id, "Ввожу заголовок…")
    for sel in [
        "input[placeholder*='аголов']",
        "textarea[placeholder*='аголов']",
        "input[name='title']",
        "[data-test='title-input']",
    ]:
        ti = page.locator(sel).first
        if ti.is_visible():
            ti.fill(title)
            _log(log_id, f"Заголовок введён ({sel})")
            break
    _snap(page)

    # ── Шаг 6: Публикуем ─────────────────────────────────────────────────
    _log(log_id, "Нажимаю «Опубликовать»…")
    pub_btn = page.locator("button:has-text('Опубликовать')").first
    pub_btn.wait_for(state="visible", timeout=15_000)
    pub_btn.click()
    page.wait_for_timeout(4000)
    _snap(page)

    final_url = page.url
    print(f"[dzen] URL после публикации: {final_url}")
    _log(log_id, "Публикация завершена!")
