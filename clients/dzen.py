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


def _publish_ui(page, publisher_id: str, video_path: str, title: str, log_id):
    """Управляет браузером для публикации видео через UI Дзена."""

    studio_url = f"https://dzen.ru/profile/editor/id/{publisher_id}/"

    # ── Шаг 1: Переходим в студию ────────────────────────────────────────
    _log(log_id, f"Переход в студию: {studio_url}")
    page.goto(studio_url, wait_until="domcontentloaded", timeout=_NAV_TIMEOUT)
    page.wait_for_timeout(2000)

    cur = page.url
    print(f"[dzen] URL после перехода: {cur}")
    if "passport.yandex" in cur or "/auth" in cur:
        raise DzenCsrfExpired(
            "Сессия истекла — авторизуйтесь снова в браузере (вкладка «Публикация»)"
        )

    # ── Диагностика: логируем все видимые кнопки и ссылки ────────────────
    try:
        import os as _os
        screenshot_path = _os.path.join(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))), "data", "dzen_studio_screenshot.jpg")
        page.screenshot(path=screenshot_path, type="jpeg", quality=80)
        print(f"[dzen] Скриншот сохранён: {screenshot_path}")
    except Exception as _e:
        print(f"[dzen] Скриншот не сохранён: {_e}")

    try:
        btns = page.locator("button, a[role='button'], [role='button']").all()
        texts = [b.inner_text().strip() for b in btns if b.is_visible()]
        print(f"[dzen] Видимые кнопки на странице: {texts}")
        _log(log_id, f"Кнопки на странице: {texts[:10]}")
    except Exception as _e:
        print(f"[dzen] Ошибка диагностики: {_e}")

    # ── Шаг 2: Кнопка создания публикации ────────────────────────────────
    _log(log_id, "Ищу кнопку создания публикации…")
    create_btn = page.locator(
        "button:has-text('Создать'), "
        "a:has-text('Создать'), "
        "button:has-text('Новая публикация'), "
        "button:has-text('Добавить'), "
        "a:has-text('Добавить'), "
        "[data-test='create-button'], "
        "button:has-text('Опубликовать'), "
        "button:has-text('Новое'), "
        "a:has-text('Новое')"
    ).first
    create_btn.wait_for(state="visible", timeout=15_000)
    create_btn.click()
    page.wait_for_timeout(1500)

    # ── Шаг 3: Выбор типа — «Короткое видео» ─────────────────────────────
    _log(log_id, "Выбираю тип «Короткое видео»…")
    for selector in [
        "text=Короткое видео",
        "text=Видео",
        "[data-type='gif']",
        "[data-type='short-video']",
        "[data-type='video']",
    ]:
        el = page.locator(selector).first
        if el.is_visible():
            el.click()
            page.wait_for_timeout(1000)
            _log(log_id, f"Выбрано: {selector}")
            break

    # ── Шаг 4: Загружаем файл ────────────────────────────────────────────
    _log(log_id, "Ищу поле загрузки файла…")
    file_input = page.locator('input[type="file"]').first
    file_input.wait_for(timeout=_UPLOAD_WAIT)
    file_input.set_input_files(video_path)
    _log(log_id, "Файл передан браузеру, жду загрузки…")

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

    # ── Шаг 6: Публикуем ─────────────────────────────────────────────────
    _log(log_id, "Нажимаю «Опубликовать»…")
    pub_btn = page.locator("button:has-text('Опубликовать')").first
    pub_btn.wait_for(state="visible", timeout=15_000)
    pub_btn.click()
    page.wait_for_timeout(4000)

    final_url = page.url
    print(f"[dzen] URL после публикации: {final_url}")
    _log(log_id, "Публикация завершена!")
