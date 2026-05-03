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


_NAV_TIMEOUT = 30_000   # ms — таймаут навигации
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
    log_id,
    batch_id=None,
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

    write_log_entry(log_id, "Дзен: Публикация запущена.")
    write_log_entry(log_id, fmt_id_msg("[dzen] {} КБ, publisher={}", len(video_data) // 1024, publisher_id), level='silent')

    # Пишем видео во временный файл с именем = заголовок (Дзен автоподставляет имя файла)
    file_name = publication_file_name(pub_title)
    write_log_entry(log_id, f"[dzen] Заголовок: {pub_title}, файл: {file_name}", level='silent')
    tmp_dir = tempfile.mkdtemp()
    video_path = os.path.join(tmp_dir, file_name)
    try:
        with open(video_path, "wb") as _f:
            _f.write(video_data)

        def _do_publish(page, _ctx):
            _publish_ui(page, publisher_id, video_path, log_id, batch_id=batch_id)

        result = _get_browser("dzen").run_pipeline_browser(_do_publish, saved_cookies)

        if not result["ok"]:
            err = result.get("error", "Неизвестная ошибка")
            if "истекла" in err or "авторизуйтесь" in err:
                raise DzenCsrfExpired(err)
            raise DzenApiError(err)

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        try:
            _get_browser("dzen").stop()
        except Exception:
            pass

    write_log_entry(log_id, "Дзен: видео опубликовано успешно")
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
        write_log_entry(None, f"[dzen] _snap: {_e}", level='silent')


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


def _try_click_captcha_checkbox(page, log_id) -> bool:
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
                        write_log_entry(log_id, f"[dzen] Капча: JS-клик {js_sel!r} в фрейме {furl or 'main'}", level='silent')
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
                    write_log_entry(log_id, f"[dzen] Капча: Playwright-клик {sel!r} в основном фрейме", level='silent')
                    clicked = True
                    break
            except Exception:
                pass

    return clicked


def _has_publish_confirm_dialog(page) -> bool:
    """Возвращает True если в DOM видна кнопка подтверждения публикации."""
    for text in ("Опубликовать после обработки", "Опубликовать после подтверждения"):
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
# Каждая запись: (имя, detect(page)->bool, handle(page, log_id, batch_id)->None)
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


def _handle_captcha_element(page, log_id, batch_id) -> None:
    """
    Обрабатывает капчу «Я не робот»: кликает чекбокс, ждёт исчезновения.
    Бросает DzenApiError если капча не прошла за 30 сек.
    """
    write_log_entry(log_id, "Дзен: Обнаружена капча, пытаюсь нажать «Я не робот».")
    _clicked = _try_click_captcha_checkbox(page, log_id)
    if not _clicked:
        write_log_entry(log_id, "Дзен: Капча обнаружена, но кликнуть чекбокс не удалось.")
        return
    write_log_entry(log_id, "Дзен: Капча — чекбокс нажат, жду исчезновения.")
    _snap(page, batch_id)
    _deadline = _time.monotonic() + 30
    while _time.monotonic() < _deadline:
        page.wait_for_timeout(1_000)
        if not _detect_captcha(page):
            write_log_entry(log_id, "Дзен: Капча пройдена.")
            _snap(page, batch_id)
            return
    _snap(page, batch_id)
    raise DzenApiError("Капча не прошла за 30 сек — публикация невозможна.")


def _handle_confirm_element(page, log_id, batch_id) -> None:
    """Обрабатывает диалог подтверждения публикации: кликает кнопку."""
    for text in ("Опубликовать после подтверждения", "Опубликовать после обработки"):
        try:
            btn = page.locator(f"button:has-text('{text}')").first
            if btn.is_visible(timeout=300):
                write_log_entry(log_id, f"Дзен: Нажимаю «{text}».")
                write_log_entry(log_id, f"[dzen] Кнопка подтверждения: «{text}»", level='silent')
                btn.click()
                _snap(page, batch_id)
                return
        except Exception:
            pass


_EXPECTED_ELEMENTS = [
    ("captcha", _detect_captcha,        _handle_captcha_element),
    ("confirm", _detect_confirm_dialog, _handle_confirm_element),
    # file_input НЕ ДОБАВЛЯТЬ сюда — после set_files() input[type=file] остаётся в DOM
    # на всё время публикации и блокирует вызов _dismiss_unknown для любых других попапов.
]


# JS-сниппет для поиска видимых попап-подобных контейнеров — структурно,
# без хардкода текстов или классов конкретных хинтов Дзена.
#
# Контейнер считается «попапом», если выполнены ОБА условия:
#   1. Он визуально «всплывающий»:
#      role ∈ {dialog, alertdialog, tooltip, menu, listbox} ИЛИ
#      computed position ∈ {fixed, absolute} с z-index ≥ 10 ИЛИ
#      DOM-тег <dialog>.
#   2. Внутри есть кликабельный «×» (button/символ ×, aria-label со словом
#      close/закр, или класс с close/dismiss).
# Возвращает массив таких контейнеров (в режиме collect=true) или результат
# первого клика по × (collect=false).
_POPUP_FIND_JS = r"""(opts) => {
    const collect = !!(opts && opts.collect);

    const CLOSE_CHARS = new Set(['\u00d7', '\u2715', '\u2716', '\u2717', 'x', 'X', '\u2613']);
    const POPUP_ROLES = new Set(['dialog', 'alertdialog', 'tooltip', 'menu', 'listbox']);

    function isVisible(el) {
        if (!el || !el.isConnected) return false;
        const cs = getComputedStyle(el);
        if (cs.display === 'none' || cs.visibility === 'hidden' || parseFloat(cs.opacity) === 0) return false;
        const r = el.getBoundingClientRect();
        if (r.width < 8 || r.height < 8) return false;
        // Не используем offsetParent — для position:fixed он всегда null,
        // что ломало детект фиксированных оверлеев.
        if (r.bottom < 0 || r.right < 0) return false;
        if (r.top > (window.innerHeight || 0) || r.left > (window.innerWidth || 0)) return false;
        return true;
    }

    function isPopupLike(el) {
        if (el.tagName === 'DIALOG' && el.open !== false) return true;
        const role = (el.getAttribute('role') || '').toLowerCase();
        if (POPUP_ROLES.has(role)) return true;
        const cs = getComputedStyle(el);
        if (cs.position === 'fixed' || cs.position === 'absolute') {
            const z = parseInt(cs.zIndex, 10);
            if (!isNaN(z) && z >= 10) return true;
        }
        return false;
    }

    // Защита от ложного закрытия рабочих UI: дропдаун выбора комментариев
    // (`select-editor`) и его опции (`select-editor__option`) не закрываем —
    // их обрабатывает _set_comments_all_users.
    const PROTECTED_CLASS_TOKENS = ['select-editor'];
    function isProtected(el) {
        let cur = el;
        while (cur && cur !== document.body) {
            const cls = (cur.getAttribute && cur.getAttribute('class')) || '';
            for (const tok of PROTECTED_CLASS_TOKENS) {
                if (cls.indexOf(tok) !== -1) return true;
            }
            cur = cur.parentElement;
        }
        return false;
    }

    // Проверяет, что строка содержит слово close/dismiss как токен
    // (а не подстроку — иначе ловит «disclosure», «enclosed» и т.п.).
    function hasCloseToken(s) {
        if (!s) return false;
        const lower = s.toLowerCase();
        const tokens = lower.split(/[\s_\-/]+/);
        for (const t of tokens) {
            if (t === 'close' || t === 'dismiss' || t === 'closebutton'
                || t.startsWith('close') && t.length <= 12
                || t.startsWith('dismiss') && t.length <= 14) {
                // Доп. отсечь явные ложные срабатывания
                if (t === 'closed' || t === 'closes' || t === 'closing') continue;
                return true;
            }
        }
        return false;
    }

    function findCloseBtn(container) {
        // 1. aria-label со словом close/закр
        const byAria = container.querySelectorAll(
            'button[aria-label*="lose" i], [role="button"][aria-label*="lose" i], '
            + 'button[aria-label*="закр" i], [role="button"][aria-label*="закр" i]'
        );
        for (const b of byAria) {
            if (!isVisible(b)) continue;
            const lbl = (b.getAttribute('aria-label') || '');
            if (hasCloseToken(lbl) || lbl.toLowerCase().indexOf('закр') !== -1) return b;
        }

        // 2. кликабельный элемент с символом ×
        const allBtns = container.querySelectorAll('button, [role="button"]');
        for (const b of allBtns) {
            if (!isVisible(b)) continue;
            const t = (b.textContent || '').trim();
            if (t.length <= 2 && (CLOSE_CHARS.has(t) || CLOSE_CHARS.has(t[0]))) return b;
        }

        // 3. кликабельный элемент с классом close/dismiss (по токену, не подстроке)
        const clickable = container.querySelectorAll(
            'button, [role="button"], [tabindex]'
        );
        for (const b of clickable) {
            if (!isVisible(b)) continue;
            if (b === container) continue;
            const cls = b.getAttribute('class') || '';
            if (!hasCloseToken(cls)) continue;
            const r = b.getBoundingClientRect();
            if (r.width > 80 || r.height > 80) continue;
            return b;
        }
        return null;
    }

    // Кандидаты — все элементы с ролью или position fixed/absolute. Перебираем
    // самые «глубокие» — клик по дочернему контейнеру предпочтителен, чтобы
    // не закрыть главную модалку, в которой попап рендерится.
    const all = Array.from(document.querySelectorAll('*'));
    const popups = [];
    for (const el of all) {
        try {
            if (!isVisible(el)) continue;
            if (!isPopupLike(el)) continue;
            if (isProtected(el)) continue;
            const close = findCloseBtn(el);
            if (!close) continue;
            popups.push({el, close});
        } catch (_) {}
    }
    if (popups.length === 0) return collect ? 0 : 'none';

    // Сортируем: сначала меньшие по площади (наиболее «листовые» попапы),
    // чтобы случайно не закрыть огромную форму-модалку.
    popups.sort((a, b) => {
        const ra = a.el.getBoundingClientRect();
        const rb = b.el.getBoundingClientRect();
        return (ra.width * ra.height) - (rb.width * rb.height);
    });

    if (collect) return popups.length;

    // Кликаем × у первого (самого маленького) попапа.
    try {
        popups[0].close.click();
        return 'clicked';
    } catch (_e) {
        return 'click_failed';
    }
}"""


def _detect_unknown_popup(page) -> bool:
    """Возвращает True, если в DOM есть видимый попап-подобный контейнер с ×.

    Структурный детект — без хардкода текстов или классов конкретных хинтов:
    role=dialog/tooltip/menu/alertdialog ИЛИ position fixed/absolute с z-index ≥ 10.
    """
    try:
        return bool(page.evaluate(_POPUP_FIND_JS, {"collect": True}))
    except Exception:
        return False


def _dismiss_unknown(page, log_id=None) -> None:
    """Закрывает неизвестный попап/диалог/хинт — но только если он реально есть.

    Алгоритм:
    0. Детектирует попап структурно (без знания конкретных текстов/классов).
       Если не найден — выходит молча, ничего не нажимает.
    1. JS-клик по × строго внутри найденного попап-контейнера.
    2. Повторяет до 3 раз — попапов может быть несколько подряд.
    3. Если после 3 попыток попап всё ещё в DOM — пишет warn-лог.
    """
    if not _detect_unknown_popup(page):
        write_log_entry(log_id, "[dzen] _dismiss_unknown: попап не обнаружен — пропускаю.", level='silent')
        return

    write_log_entry(log_id, "Дзен: Обнаружен неизвестный попап/хинт, закрываю.")

    for _attempt in range(3):
        try:
            result = page.evaluate(_POPUP_FIND_JS, {"collect": False})
            write_log_entry(log_id, f"[dzen] _dismiss_unknown: попытка {_attempt + 1} → {result!r}", level='silent')
        except Exception as _e:
            write_log_entry(log_id, f"[dzen] _dismiss_unknown: JS-evaluate упал: {_e}", level='silent')
            break

        page.wait_for_timeout(250)

        if not _detect_unknown_popup(page):
            write_log_entry(log_id, "Дзен: Неизвестный попап/хинт закрыт.")
            return

    write_log_entry(log_id, "Дзен: Не удалось закрыть неизвестный попап/хинт.", level='warn')


def _set_comments_all_users(page, log_id, batch_id=None) -> None:
    """
    Выставляет «Все пользователи» в дропдауне «Кто может комментировать».

    Стратегия 1 — JS evaluate: находит <select class*='select-editor__select'>
    (реальный класс из бандла видеоредактора Дзена) и выставляет опцию
    «Все пользователи» через DOM (работает даже если элемент визуально скрыт).
    Стратегия 2 — Playwright: кликает триггер-кнопку [class*='select-editor__trigger']
    и выбирает «Все пользователи» из открывшегося списка.
    Все ошибки ловит внутри, не пробрасывает.
    """
    _TARGET = "Все пользователи"
    write_log_entry(log_id, "Дзен: Выставляю «Все пользователи» в «Кто может комментировать».")
    try:
        # Скроллим вниз — контрол может быть ниже видимой области.
        try:
            page.evaluate("""() => {
                const form = document.querySelector('form, [class*="editor"], [class*="Editor"]');
                if (form) form.scrollTop = form.scrollHeight;
                else window.scrollBy(0, 400);
            }""")
            page.wait_for_timeout(400)
        except Exception:
            pass

        # ── Стратегия 1: JS evaluate через реальный класс select-editor ──
        # Класс нативного <select> в бандле: video-editor--select-editor__select-*
        done = page.evaluate("""(target) => {
            const sel = document.querySelector('[class*="select-editor__select"]');
            if (!sel) return 'not_found';
            const opts = Array.from(sel.options);
            const idx  = opts.findIndex(o => o.text.includes(target));
            if (idx < 0) return 'no_option';
            if (sel.options[sel.selectedIndex].text.includes(target)) return 'already';
            sel.selectedIndex = idx;
            sel.dispatchEvent(new Event('change', {bubbles: true}));
            sel.dispatchEvent(new Event('input',  {bubbles: true}));
            return 'done';
        }""", _TARGET)

        write_log_entry(log_id, f"[dzen] _set_comments: JS-evaluate → {done!r}", level='silent')

        if done == 'already':
            write_log_entry(log_id, "Дзен: Комментарии уже «Все пользователи».", level='silent')
            _snap(page, batch_id)
            return
        if done == 'done':
            write_log_entry(log_id, "Дзен: Комментарии выставлены «Все пользователи» (JS select).")
            _snap(page, batch_id)
            return

        # ── Стратегия 2: клик по триггеру select-editor, затем опция ────
        # Класс триггера в бандле: video-editor--select-editor__trigger-*
        trigger = page.locator("[class*='select-editor__trigger']").first
        trigger_found = False
        try:
            trigger.wait_for(state="visible", timeout=4_000)
            trigger_found = True
        except Exception:
            pass

        if not trigger_found:
            write_log_entry(log_id, "Дзен: select-editor__trigger не найден — продолжаю.", level='silent')
            return

        current_text = trigger.inner_text() or ""
        write_log_entry(log_id, f"[dzen] _set_comments: триггер найден, текущий: {current_text!r}", level='silent')

        if _TARGET in current_text:
            write_log_entry(log_id, "Дзен: Комментарии уже «Все пользователи».", level='silent')
            _snap(page, batch_id)
            return

        trigger.click()
        page.wait_for_timeout(400)

        option = page.locator(
            "[role='option']:has-text('Все пользователи'), "
            "li:has-text('Все пользователи'), "
            "div[role='option']:has-text('Все пользователи'), "
            "*:has-text('Все пользователи'):visible"
        ).first
        try:
            option.wait_for(state="visible", timeout=5_000)
            option.click()
            write_log_entry(log_id, "Дзен: Комментарии выставлены «Все пользователи» (trigger+option).")
            _snap(page, batch_id)
        except Exception as _e:
            write_log_entry(log_id, "Дзен: Не удалось выбрать «Все пользователи» — продолжаю.", level='silent')
            write_log_entry(log_id, f"[dzen] Ошибка выбора опции: {_e}", level='silent')
    except Exception as _e:
        write_log_entry(log_id, "Дзен: Ошибка при настройке комментариев — продолжаю.", level='silent')
        write_log_entry(log_id, f"[dzen] Ошибка _set_comments_all_users: {_e}", level='silent')


def _handle_popups(page, log_id=None, batch_id=None) -> None:
    """
    Проверяет страницу на попапы, хинты, тултипы и диалоги.
    Сначала сверяет со списком _EXPECTED_ELEMENTS — если совпадение найдено,
    вызывает соответствующий обработчик и возвращает управление.
    Если ни один известный элемент не обнаружен — закрывает неизвестный попап.
    """
    for name, detect, handle in _EXPECTED_ELEMENTS:
        if detect(page):
            write_log_entry(log_id, f"[dzen] Ожидаемый элемент: {name}", level='silent')
            if handle is not None:
                handle(page, log_id, batch_id)
            return
    _dismiss_unknown(page, log_id)


def _publish_ui(page, publisher_id: str, video_path: str, log_id, batch_id=None):
    """Управляет браузером для публикации видео через UI Дзена."""

    studio_url = f"https://dzen.ru/profile/editor/id/{publisher_id}/"

    # ── Шаг 1: Переходим в студию ────────────────────────────────────────
    write_log_entry(log_id, "Дзен: Переход в студию.")
    write_log_entry(log_id, f"[dzen] URL студии: {studio_url}", level='silent')
    page.goto(studio_url, wait_until="domcontentloaded", timeout=_NAV_TIMEOUT)
    _snap(page, batch_id)

    cur = page.url
    write_log_entry(log_id, f"[dzen] URL после перехода: {cur}", level='silent')
    if "passport.yandex" in cur or "/auth" in cur:
        raise DzenCsrfExpired(
            "Сессия истекла — авторизуйтесь снова в браузере (вкладка «Публикация»)"
        )

    # ── Закрываем модальный overlay если есть (онбординг, донаты и т.п.) ─
    try:
        overlay = page.locator("[data-testid='modal-overlay']").first
        if overlay.is_visible():
            write_log_entry(log_id, "Дзен: Закрываю модальное окно.")
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
    _handle_popups(page, log_id, batch_id)

    # ── Шаг 2: Кнопка «+» (плюсик) в правом верхнем углу ────────────────
    write_log_entry(log_id, "Дзен: Ищу кнопку «+» для создания публикации.")
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
    write_log_entry(log_id, "Дзен: Кнопка «+» нажата, жду меню.")
    _snap(page, batch_id)

    # Перед шагом 3: закрываем любой неожиданный попап/хинт.
    _dismiss_unknown(page, log_id)

    # ── Шаг 3: «Загрузить видео» из выпадающего меню ─────────────────────
    write_log_entry(log_id, "Дзен: Выбираю «Загрузить видео».")
    upload_item = page.get_by_text("Загрузить видео", exact=True).first
    try:
        upload_item.wait_for(state="visible", timeout=180_000)
    except Exception:
        write_log_entry(log_id, "Дзен: exact-match не нашёл — пробую contains.")
        upload_item = page.locator("text=Загрузить видео").first
        upload_item.wait_for(state="visible", timeout=180_000)
    upload_item.click()
    write_log_entry(log_id, "Дзен: «Загрузить видео» нажато")
    _snap(page, batch_id)

    # Перед шагом 4: закрываем любой неожиданный попап/хинт.
    _dismiss_unknown(page, log_id)

    # ── Шаг 4: Загружаем файл ────────────────────────────────────────────
    write_log_entry(log_id, "Дзен: Ищу поле загрузки файла.")
    # Ждём появления кнопки ДО входа в expect_file_chooser:
    # если войти до того, как кнопка видна, click() зависает внутри with-блока
    # и expect_file_chooser истекает раньше, чем диалог успевает открыться.
    choose_btn = page.get_by_text("Выбрать видео", exact=False).first
    choose_btn.wait_for(state="visible", timeout=180_000)
    write_log_entry(log_id, "Дзен: Кнопка «Выбрать видео» найдена, открываю диалог выбора файла.")
    with page.expect_file_chooser(timeout=180_000) as fc_info:
        choose_btn.click()
    file_chooser = fc_info.value
    file_chooser.set_files(video_path)
    write_log_entry(log_id, "Дзен: Файл передан браузеру, жду загрузки.")
    write_log_entry(log_id, f"[dzen] Файл: {os.path.basename(video_path)}", level='silent')
    _snap(page, batch_id)

    # Ждём одно из двух:
    #   a) ?videoEditorPublicationId=...  — редактор открылся, нужно кликать «Опубликовать»
    #   b) ?state=published               — Дзен опубликовал сам, ничего больше не нужно
    # Во время ожидания периодически закрываем любые неожиданные попапы.
    write_log_entry(log_id, "Дзен: Жду открытия редактора видео или авто-публикации.")
    _editor_opened = False
    _auto_published = False
    _url_deadline = _time.monotonic() + _UPLOAD_WAIT / 1000
    while _time.monotonic() < _url_deadline:
        _cur = page.url
        if "state=published" in _cur:
            _auto_published = True
            write_log_entry(log_id, "Дзен: Видео опубликовано автоматически.")
            write_log_entry(log_id, f"[dzen] URL авто-публикации: {_cur}", level='silent')
            break
        if "videoEditorPublicationId" in _cur:
            _editor_opened = True
            write_log_entry(log_id, "Дзен: Редактор видео открылся.")
            write_log_entry(log_id, f"[dzen] URL редактора: {_cur}", level='silent')
            break
        _handle_popups(page, log_id, batch_id)
        page.wait_for_timeout(1_500)

    if _auto_published:
        # Видео уже опубликовано — пропускаем шаги 5-7
        _snap(page, batch_id)
        write_log_entry(log_id, "Дзен: Публикация завершена.")
        return

    if not _editor_opened:
        # Запасной вариант: ждём поле заголовка или кнопку в диалоге
        write_log_entry(log_id, "Дзен: URL редактора не появился, жду форму.")
        try:
            page.wait_for_selector(
                "input[placeholder*='аголов'], "
                "textarea[placeholder*='аголов'], "
                "button:has-text('Опубликовать после обработки')",
                timeout=15_000,
            )
        except Exception:
            write_log_entry(log_id, "Дзен: Форма не обнаружена — продолжаю по таймауту.")
            page.wait_for_timeout(5000)
    _snap(page, batch_id)

    # Редактор открылся — закрываем все неожиданные попапы (любые подсказки,
    # хинты, уведомления Дзена), которые могут мешать заполнению формы.
    _handle_popups(page, log_id, batch_id)

    # ── Шаг 5: Заполняем теги ────────────────────────────────────────────
    write_log_entry(log_id, "Дзен: Заполняю теги.")
    write_log_entry(log_id, f"[dzen] Теги: {tags()}", level='silent')
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
        write_log_entry(log_id, "Дзен: Теги заполнены")
        _snap(page, batch_id)
    except Exception as _e:
        write_log_entry(log_id, "Дзен: Не удалось заполнить теги — продолжаю.")
        write_log_entry(log_id, f"[dzen] Ошибка тегов: {_e}", level='silent')

    # Перед шагом 5.1: закрываем хинт «Уже можно публиковать» (он часто
    # всплывает после ввода тегов и перекрывает дропдаун комментариев).
    _dismiss_unknown(page, log_id)

    # ── Шаг 5.1: Выставляем «Все пользователи» в «Кто может комментировать»
    # Сразу после тегов — контрол виден в той же модалке «Публикация ролика».
    _set_comments_all_users(page, log_id, batch_id)

    # Перед шагом 6: ещё раз закрываем любые всплывшие хинты/попапы.
    _dismiss_unknown(page, log_id)

    # ── Шаг 6: Публикуем ─────────────────────────────────────────────────
    write_log_entry(log_id, "Дзен: Нажимаю «Опубликовать».")
    pub_btn = page.locator("button:has-text('Опубликовать')").first
    pub_btn.wait_for(state="visible", timeout=180_000)

    # Перед кликом — проверяем на капчу и закрываем всё лишнее.
    # Капча от VK блокирует pointer events на всей странице, поэтому
    # она должна быть обработана ДО попытки клика.
    _handle_popups(page, log_id, batch_id)

    try:
        pub_btn.click(timeout=30_000)
    except Exception as _click_err:
        write_log_entry(log_id, "[dzen] Обычный клик не прошёл — пробую JS-клик.", level='silent')
        write_log_entry(log_id, f"[dzen] Причина: {_click_err}", level='silent')
        try:
            page.evaluate(
                "() => { const b = document.querySelector('[data-testid=\"publish-btn\"]') "
                "|| document.querySelector('button[type=\"submit\"]'); if(b) b.click(); }"
            )
        except Exception as _js_err:
            write_log_entry(log_id, f"[dzen] JS-клик тоже не прошёл: {_js_err}", level='silent')

    # Ждём появления диалога подтверждения или капчи до 12 секунд.
    # Если за 12 сек ничего не появилось — продолжаем в шаг 7.
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

    # ── Шаг 7: Обрабатываем попапы, диалоги, хинты (25 секунд) ──────────
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
    _step7_done = False

    while _time.monotonic() < _dialog_deadline:
        # Обрабатываем любые попапы/диалоги/хинты через единый список ожидаемых элементов.
        # DzenApiError из _handle_captcha_element пробросится наружу автоматически.
        _handle_popups(page, log_id, batch_id)

        _check_error_toast()

        # Проверяем финальное подтверждение публикации
        try:
            success_now = page.locator(
                "[class*='toast']:has-text('опубликован'), "
                "[class*='notification']:has-text('опубликован'), "
                "[data-testid='publish-success']"
            ).first
            if success_now.is_visible():
                write_log_entry(log_id, "Дзен: Публикация подтверждена в шаге 7.")
                _step7_done = True
                break
        except Exception:
            pass

        page.wait_for_timeout(_DIALOG_POLL)

    write_log_entry(log_id, "Дзен: Шаг 7 завершён, жду подтверждения публикации.")

    # Перед шагом 8: закрываем любой неожиданный попап/хинт.
    _dismiss_unknown(page, log_id)

    # ── Шаг 8: Ожидаем подтверждения публикации ──────────────────────────
    _PUBLISH_CONFIRM_TIMEOUT = 60_000  # ms — полный таймаут ожидания
    _CONFIRM_POLL = 2_000              # ms — интервал опроса

    url_before = page.url
    confirmed = False

    # Быстрая проверка: браузер уже на странице подтверждения ещё до цикла
    if "state=published" in url_before or "state=pending" in url_before:
        state_label = "state=published" if "state=published" in url_before else "state=pending"
        confirmed = True
        write_log_entry(log_id, f"Дзен: URL → {state_label} — публикация подтверждена.")
        write_log_entry(log_id, f"[dzen] Полный URL: {url_before}", level='silent')

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
    while _time.monotonic() < _confirm_deadline and not confirmed:
        _iter += 1
        if _iter % _snap_every == 1:   # первый снимок сразу, потом каждые 6 сек
            _snap(page, batch_id)

        # 1. CSS-проверка
        try:
            el = page.locator(css_success_selector).first
            if el.is_visible():
                confirmed = True
                write_log_entry(log_id, "Дзен: Уведомление об успешной публикации получено (CSS).")
                write_log_entry(log_id, f"[dzen] URL: {page.url}", level='silent')
                break
        except Exception:
            pass

        # 2. Текстовая проверка
        for pat in text_success_patterns:
            try:
                el = page.locator(pat).first
                if el.is_visible():
                    confirmed = True
                    write_log_entry(log_id, "Дзен: Публикация подтверждена (текст).")
                    write_log_entry(log_id, f"[dzen] Совпадение: {pat!r}", level='silent')
                    break
            except Exception:
                pass
        if confirmed:
            break

        # 2b. Проверка тост-ошибок Дзена — завершаем сразу, не ждём таймаута
        _check_error_toast()

        # 2c. Обрабатываем попапы/диалоги/хинты (капча может появиться и здесь)
        _handle_popups(page, log_id, batch_id)

        # 3. Проверка URL
        url_now = page.url
        if url_now != url_before:
            # state=published / state=pending — Дзен подтвердил публикацию
            if "state=published" in url_now or "state=pending" in url_now:
                state_label = "state=published" if "state=published" in url_now else "state=pending"
                confirmed = True
                write_log_entry(log_id, f"Дзен: URL → {state_label} — публикация подтверждена.")
                break
            video_match = re.search(r"/video/|/shorts/|/watch\?", url_now)
            if video_match or "editor" not in url_now:
                confirmed = True
                write_log_entry(log_id, "Дзен: Публикация подтверждена (смена URL).")
                write_log_entry(log_id, f"[dzen] URL сменился: {url_now}", level='silent')
                break

        page.wait_for_timeout(_CONFIRM_POLL)

    page.remove_listener("framenavigated", _on_navigate)

    if not confirmed:
        # Финальный URL-снимок
        url_after = page.url
        write_log_entry(log_id, f"[dzen] URL до публикации: {url_before}", level='silent')
        write_log_entry(log_id, f"[dzen] URL после публикации: {url_after}", level='silent')
        if "state=published" in url_after or "state=pending" in url_after:
            state_label = "state=published" if "state=published" in url_after else "state=pending"
            confirmed = True
            write_log_entry(log_id, f"Дзен: URL → {state_label} — публикация подтверждена (финал).")
            write_log_entry(log_id, f"[dzen] Полный URL: {url_after}", level='silent')
        else:
            video_url_pattern = re.search(r"/video/|/shorts/|/watch\?", url_after)
            if video_url_pattern and url_after != url_before:
                confirmed = True
                write_log_entry(log_id, "Дзен: Публикация подтверждена (видео-страница).")
                write_log_entry(log_id, f"[dzen] URL видео: {url_after}", level='silent')
            elif url_after != url_before and "editor" not in url_after:
                confirmed = True
                write_log_entry(log_id, "Дзен: Публикация предположительно подтверждена.")
                write_log_entry(log_id, f"[dzen] URL сменился: {url_after}", level='silent')

    _snap(page, batch_id)

    if not confirmed:
        _check_error_toast()  # бросает DzenApiError если есть явная ошибка
        _snap(page, batch_id)
        raise DzenApiError(
            "Подтверждение публикации не получено за 60 с — "
            "видео предположительно в черновиках. Проверьте вручную."
        )

    write_log_entry(log_id, f"[dzen] URL после публикации: {page.url}", level='silent')
    write_log_entry(log_id, "Дзен: Публикация завершена.")
