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

        result = _get_browser("dzen").run_pipeline_browser(_do_publish, saved_cookies, batch_id=batch_id, category=category)

        if not result["ok"]:
            err = result.get("error", "Неизвестная ошибка")
            if "истекла" in err or "авторизуйтесь" in err:
                raise DzenCsrfExpired(err)
            raise DzenApiError(err)

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
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

_CAPTCHA_URL_KEYWORDS = (
    "not_robot_captcha", "smartcaptcha", "yandexcloud",
    "captcha.yandex", "recaptcha", "id.vk.com",
)

_CAPTCHA_DOM_KEYWORDS = (
    "не робот", "not a robot", "подтвердите", "я не робот",
    "captcha", "капча",
)

def _has_captcha_frame(page) -> bool:
    """Возвращает True если в page есть активный iframe капчи (по URL)."""
    try:
        for frame in page.frames:
            furl = frame.url.lower()
            if any(kw in furl for kw in _CAPTCHA_URL_KEYWORDS):
                return True
    except Exception:
        pass
    return False

def _has_captcha_dom(page) -> bool:
    """Резервная проверка: капча обнаружена по тексту страницы или DOM-классам."""
    try:
        body = page.locator("body").inner_text(timeout=1000)
        body_lc = body.lower()
        if any(kw in body_lc for kw in _CAPTCHA_DOM_KEYWORDS):
            return True
    except Exception:
        pass
    try:
        for sel in (
            "[class*='captcha']",
            "[class*='Captcha']",
            "[id*='captcha']",
            "iframe[src*='captcha']",
            "iframe[src*='smartcaptcha']",
        ):
            try:
                if page.locator(sel).first.is_visible(timeout=300):
                    return True
            except Exception:
                pass
    except Exception:
        pass
    return False

def _try_click_captcha_checkbox(page, category, batch_id=None) -> bool:
    """
    Пытается кликнуть чекбокс капчи «Я не робот» во всех фреймах и в основном.
    Возвращает True если клик выполнен хотя бы в одном месте.
    """
    checkbox_selectors = [
        'input[type="checkbox"]',
        '[class*="Checkbox"] input',
        '[class*="checkbox"] input',
        'label',
        "[role='checkbox']",
    ]
    clicked = False

    try:
        for frame in page.frames:
            furl = frame.url.lower()
            if not any(kw in furl for kw in _CAPTCHA_URL_KEYWORDS):
                continue
            for js_sel in checkbox_selectors[:3]:
                try:
                    done = frame.evaluate(
                        f'() => {{ const el = document.querySelector({repr(js_sel)}); '
                        f'if (el) {{ el.click(); return true; }} return false; }}'
                    )
                    if done:
                        write_log_entry(batch_id, category, f"Капча: JS-клик {js_sel!r} в фрейме {furl or 'main'}", level='silent')
                        clicked = True
                        break
                except Exception:
                    pass
            if clicked:
                break
    except Exception:
        pass

    if not clicked:
        for sel in checkbox_selectors:
            try:
                el = page.locator(sel).first
                if el.is_visible(timeout=300):
                    el.click(force=True, timeout=2000)
                    write_log_entry(batch_id, category, f"Капча: Playwright-клик {sel!r} в основном фрейме", level='silent')
                    clicked = True
                    break
            except Exception:
                pass

    return clicked

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

# ---------------------------------------------------------------------------
# Известные ожидаемые элементы — список признаков и действий
#
# Каждая запись: (имя, detect(page)->bool, handle(page, category, batch_id)->None)
# handle=None означает «обнаружен, действий не требует — просто не закрывать».
# Всё, что не попало в этот список — закрывается без разбора (_dismiss_unknown).
# ---------------------------------------------------------------------------

def _detect_captcha(page) -> bool:
    return _has_captcha_frame(page) or _has_captcha_dom(page)

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
    Бросает DzenApiError если капча не прошла за 30 сек.
    """
    write_log_entry(batch_id, category, "Дзен: Обнаружена капча, пытаюсь нажать «Я не робот».")
    _clicked = _try_click_captcha_checkbox(page, category, batch_id)
    if not _clicked:
        write_log_entry(batch_id, category, "Дзен: Капча обнаружена, но кликнуть чекбокс не удалось.")
        return
    write_log_entry(batch_id, category, "Дзен: Капча — чекбокс нажат, жду исчезновения.")
    _snap(page, batch_id)
    _deadline = _time.monotonic() + 30
    while _time.monotonic() < _deadline:
        page.wait_for_timeout(1_000)
        if not _detect_captcha(page):
            write_log_entry(batch_id, category, "Дзен: Капча пройдена.")
            _snap(page, batch_id)
            return
    _snap(page, batch_id)
    raise DzenApiError("Капча не прошла за 30 сек — публикация невозможна.")

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

def _click_primary_publish_control(page, category, batch_id=None) -> bool:
    """Закрывает попапы и нажимает основную кнопку «Опубликовать». Возвращает True если кликнули."""
    _handle_popups(page, category, batch_id)
    pub_btn = _find_primary_publish_control(page)
    if pub_btn is None:
        return False
    write_log_entry(batch_id, category, "Дзен: Элемент публикации найден, нажимаю.")
    try:
        pub_btn.click(timeout=3_000)
    except Exception as _click_err:
        write_log_entry(batch_id, category, "[dzen] Обычный клик не прошёл — пробую JS-клик.", level='silent')
        write_log_entry(batch_id, category, f"Причина: {_click_err}", level='silent')
        try:
            page.evaluate(
                """() => {
                    const byTestId = document.querySelector('[data-testid="publish-btn"]');
                    if (byTestId) { byTestId.click(); return; }
                    for (const el of document.querySelectorAll('button, [role="button"], a')) {
                        const t = (el.textContent || '').trim();
                        if (/^Опубликовать(?: после обработки)?$/i.test(t)) {
                            el.click();
                            return;
                        }
                    }
                }"""
            )
        except Exception as _js_err:
            write_log_entry(batch_id, category, f"JS-клик тоже не прошёл: {_js_err}", level='silent')
            return False
    _snap(page, batch_id)
    return True

def _retry_publish_if_button_visible(page, category, batch_id, url_step7_start, reason: str) -> bool:
    """Повторный клик, если публикация ещё не ушла, а CTA всё ещё на экране."""
    if _dzen_step7_success_without_click(page, url_step7_start):
        return False
    if _find_primary_publish_control(page) is None:
        return False
    write_log_entry(batch_id, category, f"Дзен: {reason}")
    return _click_primary_publish_control(page, category, batch_id)

_EXPECTED_ELEMENTS = [
    ("captcha", _detect_captcha,        _handle_captcha_element),
    ("confirm", _detect_confirm_dialog, _handle_confirm_element),
    # file_input НЕ ДОБАВЛЯТЬ сюда — после set_files() input[type=file] остаётся в DOM
    # на всё время публикации и блокирует вызов _dismiss_unknown для любых других попапов.
]

_HINT_CLOSE_SELECTOR = "[class*='helper-tooltip__closeButton']"

def _dismiss_unknown(page, category=None, batch_id=None) -> None:
    """Закрывает хинт-попап Дзена — только если он реально есть.

    Единственный путь: клик по `[class*='helper-tooltip__closeButton']` через
    Playwright Locator (настоящий мышиный клик, который React гарантированно
    ловит). После клика — реальная верификация `is_visible()`. Если хинта нет
    — выходит молча.

    Структурный fallback `_POPUP_FIND_JS` отключён: на практике он цеплялся
    за постоянные мелкие кнопки редактора (не попапы), генерил ложные
    'clicked' и спамил warn. В бандле video-editor класс закрытия хинта
    ровно один — `helper-tooltip__closeButton`. Если Дзен введёт новый тип
    хинта — добавим его конкретный селектор сюда же.
    """
    # При log_id=None все info/warn должны идти как silent (правило 4 конвенций).
    _user_lvl = 'info' if batch_id else 'silent'
    _warn_lvl = 'warn' if batch_id else 'silent'

    hint_was_seen = False
    for _attempt in range(3):
        try:
            btn = page.locator(_HINT_CLOSE_SELECTOR).first
            if not btn.is_visible(timeout=300):
                break
        except Exception:
            break

        hint_was_seen = True

        # Снимок для верификации (короткий таймаут, чтобы не висеть)
        try:
            cls_before = btn.get_attribute("class", timeout=300) or ""
        except Exception:
            cls_before = ""

        write_log_entry(batch_id, category, f"Дзен: Закрываю хинт (попытка {_attempt + 1}).", level=_user_lvl)
        write_log_entry(batch_id, category, f"hint close target class={cls_before!r}", level='silent')

        try:
            url_before_click = page.url
        except Exception:
            url_before_click = ""

        try:
            btn.click(timeout=2_000)
        except Exception as _e:
            write_log_entry(batch_id, category, f"hint click failed: {_e}", level='silent')
            # Во время публикации страница может мгновенно перейти в список материалов.
            # В этом случае close-кнопка хинта естественно detatch'ится, это не ошибка.
            try:
                url_now = page.url
            except Exception:
                url_now = ""
            try:
                still_visible = page.locator(_HINT_CLOSE_SELECTOR).first.is_visible(timeout=200)
            except Exception:
                still_visible = False

            if not still_visible:
                write_log_entry(batch_id, category, "Дзен: Хинт закрыт.", level=_user_lvl)
                return

            left_editor = (
                ("videoEditorPublicationId" in (url_before_click or ""))
                and ("videoEditorPublicationId" not in (url_now or ""))
            ) or ("state=published" in (url_now or "")) or ("state=pending" in (url_now or ""))
            if left_editor:
                write_log_entry(batch_id, category, f"hint close interrupted by navigation: {url_now}", level='silent')
                return

            write_log_entry(batch_id, category, "[dzen] hint click failed, retrying.", level='silent')
            continue

        page.wait_for_timeout(300)

        # Реальная верификация: элемент исчез?
        try:
            still_visible = page.locator(_HINT_CLOSE_SELECTOR).first.is_visible(timeout=200)
        except Exception:
            still_visible = False

        if not still_visible:
            write_log_entry(batch_id, category, "Дзен: Хинт закрыт.", level=_user_lvl)
            return

        write_log_entry(batch_id, category, "[dzen] хинт всё ещё виден после клика — повтор.", level='silent')

    if hint_was_seen:
        write_log_entry(batch_id, category, "Дзен: Хинт helper-tooltip не закрылся за 3 попытки.", level=_warn_lvl)

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

def _handle_popups(page, category=None, batch_id=None) -> None:
    """
    Проверяет страницу на попапы, хинты, тултипы и диалоги.
    Сначала сверяет со списком _EXPECTED_ELEMENTS — если совпадение найдено,
    вызывает соответствующий обработчик и возвращает управление.
    Если ни один известный элемент не обнаружен — закрывает неизвестный попап.
    """
    for name, detect, handle in _EXPECTED_ELEMENTS:
        if detect(page):
            write_log_entry(batch_id, category, f"Ожидаемый элемент: {name}", level='silent')
            if handle is not None:
                handle(page, category, batch_id)
            return
    _dismiss_unknown(page, category, batch_id)

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
    if "passport.yandex" in cur or "/auth" in cur:
        raise DzenCsrfExpired(
            "Сессия истекла — авторизуйтесь снова в браузере (вкладка «Публикация»)"
        )

    # ── Закрываем модальный overlay если есть (онбординг, донаты и т.п.) ─
    try:
        overlay = page.locator("[data-testid='modal-overlay']").first
        if overlay.is_visible():
            write_log_entry(batch_id, category, "Дзен: Закрываю модальное окно.")
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

    # Перед шагом 2: обрабатываем любые попапы/диалоги/хинты
    _handle_popups(page, category, batch_id)

    # ── Шаг 2: Кнопка «+» (плюсик) в правом верхнем углу ────────────────
    write_log_entry(batch_id, category, "Дзен: Ищу кнопку «+» для создания публикации.")
    plus_btn = page.locator(
        "[class*='addButton'], "
        "[class*='author-studio-header__addButton'], "
        "[data-testid='add-publication-button'], "
        "button[aria-label*='Создать'], "
        "button[aria-label*='создать'], "
        "button[title*='Создать'], "
        "button[aria-label*='Create']"
    ).first
    plus_btn.wait_for(state="visible", timeout=180_000)
    plus_btn.click()
    write_log_entry(batch_id, category, "Дзен: Кнопка «+» нажата, жду меню.")
    _snap(page, batch_id)

    # Перед шагом 3: закрываем любой неожиданный попап/хинт.
    _dismiss_unknown(page, category, batch_id)

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
    _dismiss_unknown(page, category, batch_id)

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
        _handle_popups(page, category, batch_id)
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
    _handle_popups(page, category, batch_id)

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
    _dismiss_unknown(page, category, batch_id)

    # ── Шаг 6: Выставляем «Все пользователи» в «Кто может комментировать» ─
    # Сразу после тегов — контрол виден в той же модалке «Публикация ролика».
    _set_comments_all_users(page, category, batch_id)

    # Перед шагом 7: ещё раз закрываем любые всплывшие хинты/попапы.
    _dismiss_unknown(page, category, batch_id)

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
        _handle_popups(page, category, batch_id)
        page.wait_for_timeout(1_500)

    if pub_btn is None and not _dzen_step7_success_without_click(page, url_step7_start):
        raise DzenApiError(
            "Не дождались кнопки публикации и не обнаружили успешный редирект за 3 минуты."
        )

    if pub_btn is not None:
        _click_primary_publish_control(page, category, batch_id)

    # Ждём появления диалога подтверждения или капчи до 12 секунд.
    # Если за 12 сек ничего не появилось — продолжаем в шаг 8.
    _CONFIRM_OR_CAPTCHA_SEL = (
        "button:has-text('Опубликовать после подтверждения'), "
        "button:has-text('Опубликовать после обработки'), "
        "iframe[src*='captcha'], iframe[src*='smartcaptcha'], "
        "[class*='captcha'], [class*='Captcha'], [id*='captcha']"
    )
    try:
        page.wait_for_selector(_CONFIRM_OR_CAPTCHA_SEL, timeout=12_000)
    except Exception:
        pass
    _snap(page, batch_id)

    # ── Шаг 8: Обрабатываем попапы, диалоги, хинты (25 секунд) ──────────
    # Каждую итерацию вызываем _handle_popups — он сверяет со списком
    # _EXPECTED_ELEMENTS (капча, диалог подтверждения, файловый input)
    # и либо обрабатывает известный элемент, либо закрывает неизвестный.
    _DIALOG_WINDOW = 25_000  # ms
    _DIALOG_POLL   = 2_000   # ms

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
        _handle_popups(page, category, batch_id)

        _check_error_toast()

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
    _dismiss_unknown(page, category, batch_id)
    if not _step8_done:
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
        _handle_popups(page, category, batch_id)

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

        # 3. Проверка URL
        url_now = page.url
        if url_now != url_before:
            # state=published / state=pending — Дзен подтвердил публикацию
            if "state=published" in url_now or "state=pending" in url_now:
                state_label = "state=published" if "state=published" in url_now else "state=pending"
                confirmed = True
                write_log_entry(batch_id, category, f"Дзен: URL → {state_label} — публикация подтверждена.")
                break
            video_match = re.search(r"/video/|/shorts/|/watch\?", url_now)
            if video_match or "editor" not in url_now:
                confirmed = True
                write_log_entry(batch_id, category, "Дзен: Публикация подтверждена (смена URL).")
                write_log_entry(batch_id, category, f"URL сменился: {url_now}", level='silent')
                break

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
