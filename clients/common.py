"""
Общие утилиты Playwright-клиентов публикации (dzen, rutube, vkvideo).

Единый контракт: handle_popups(whitelist) → dismiss_unknown.
Платформенное — whitelist и стратегия dismiss_unknown:
  • Дзен — modal-overlay: dismiss_overlay_strict; hint: dismiss_dzen_hint
  • Rutube / VK — dismiss_overlay_strict (снаружи → Escape → ×, без ретраев)
"""

from __future__ import annotations

import time as _time
from collections.abc import Callable, Sequence
from typing import Any

from log import write_log_entry

_SESSION_MSG = (
    "Сессия истекла — авторизуйтесь снова в браузере (вкладка «Публикация»)"
)

# (имя, detect(page)->bool, handle(page, category, batch_id)|None)
WhitelistEntry = tuple[str, Callable[..., bool], Callable[..., None] | None]

DismissUnknown = Callable[..., None]

_DZEN_HINT_CLOSE_SELECTOR = "[class*='helper-tooltip__closeButton']"

_SAFE_FIELD_JS = """
(inset) => {
  const w = window.innerWidth;
  const h = window.innerHeight;
  if (w < inset * 3 || h < inset * 3) return null;
  const points = [
    [inset, inset],
    [w - inset, inset],
    [inset, h - inset],
    [w - inset, h - inset],
    [inset, Math.round(h / 2)],
    [w - inset, Math.round(h / 2)],
    [Math.round(w / 2), inset],
    [Math.round(w / 2), h - inset],
  ];
  const skip = (el) => {
    if (!el || el.nodeType !== 1) return true;
    const tag = el.tagName;
    if (['A', 'BUTTON', 'INPUT', 'SELECT', 'TEXTAREA', 'LABEL', 'OPTION'].includes(tag)) {
      return true;
    }
    if (el.isContentEditable) return true;
    if (el.closest('a, button, input, select, textarea, [role="button"], [role="link"], [role="menuitem"]')) {
      return true;
    }
    return false;
  };
  for (const [x, y] of points) {
    const top = document.elementFromPoint(x, y);
    if (skip(top)) continue;
    return { x, y };
  }
  return null;
}
"""


class OverlayNotDismissedError(RuntimeError):
    """Оверлей остался на экране после полной цепочки закрывающих действий."""


def _page_url(page) -> str:
    try:
        return page.url.lower()
    except Exception:
        return ""


def _config_id_str(value) -> str:
    """Нормализует id из JSON-конфига таргета (str или int)."""
    if value is None:
        return ""
    return str(value).strip()


def _visible(page, locator, timeout_ms: int = 400) -> bool:
    try:
        return locator.first.is_visible(timeout=timeout_ms)
    except Exception:
        return False


def _url_indicates_login(url: str, platform: str) -> bool:
    if not url:
        return False
    if platform == "rutube":
        return (
            "rutube.ru/login" in url
            or "passport.rutube" in url
            or "/auth" in url
            or "passport" in url
        )
    if platform == "dzen":
        return "passport.yandex" in url or "/auth" in url
    if platform == "vkvideo":
        return (
            "vk.com/login" in url
            or "login.vk" in url
            or "passport.vk" in url
            or "oauth.vk" in url
            or "id.vk.com/auth" in url
            or "id.vk.com/login" in url
            or ("id.vk.com" in url and "/auth" in url)
        )
    return False


def _dzen_publish_access_denied(page, publisher_id: str | None = None) -> bool:
    """Нет доступа к студии: URL не editor нужного publisher_id."""
    url = _page_url(page)
    if publisher_id:
        pid = _config_id_str(publisher_id).lower()
        return f"/profile/editor/id/{pid}" not in url
    return "/profile/editor/" not in url


def _rutube_publish_access_denied(page) -> bool:
    return "studio.rutube.ru" not in _page_url(page)


def _vkvideo_publish_access_denied(page, club_id: str | None = None) -> bool:
    url = _page_url(page)
    if "cabinet.vkvideo.ru" not in url:
        return True
    if club_id:
        normalized = _config_id_str(club_id).lstrip("@")
        if normalized and f"club{normalized}" not in url.replace("@", ""):
            return True
    return False


def login_screen_visible(page, platform: str, **context) -> bool:
    """True если URL указывает на экран входа или нет доступа к кабинету."""
    if _url_indicates_login(_page_url(page), platform):
        return True
    if platform == "dzen":
        return _dzen_publish_access_denied(page, context.get("publisher_id"))
    if platform == "rutube":
        return _rutube_publish_access_denied(page)
    if platform == "vkvideo":
        return _vkvideo_publish_access_denied(page, context.get("club_id"))
    return False


def raise_if_login_required(page, platform: str, **context) -> None:
    """Бросает *CsrfExpired платформы, если виден экран входа или нет доступа к кабинету."""
    if not login_screen_visible(page, platform, **context):
        return
    if platform == "dzen":
        from clients.dzen import DzenCsrfExpired

        raise DzenCsrfExpired(_SESSION_MSG)
    if platform == "rutube":
        from clients.rutube import RutubeCsrfExpired

        raise RutubeCsrfExpired(_SESSION_MSG)
    if platform == "vkvideo":
        from clients.vkvideo import VkVideoCsrfExpired

        raise VkVideoCsrfExpired(_SESSION_MSG)


def handle_popups(
    page,
    whitelist: Sequence[WhitelistEntry],
    dismiss_unknown: DismissUnknown,
    batch_id=None,
    category=None,
    *,
    allow_dismiss: bool = True,
) -> None:
    """
    Паттерн из dzen._handle_popups: whitelist → иначе dismiss_unknown.
    handle=None — элемент известен, не закрывать.
    allow_dismiss=False — только whitelist (ожидание целевого UI, без dismiss).
    """
    for name, detect, handle in whitelist:
        try:
            if not detect(page):
                continue
        except Exception:
            continue
        write_log_entry(batch_id, category, f"whitelist: {name}", level="silent")
        if handle is not None:
            handle(page, category, batch_id)
        return
    if allow_dismiss:
        dismiss_unknown(page, category, batch_id)


def _try_close_selectors(page, selectors: Sequence[str]) -> bool:
    for sel in selectors:
        try:
            btn = page.locator(sel).first
            if btn.is_visible(timeout=150):
                btn.click(timeout=2_000)
                return True
        except Exception:
            pass
    return False


def dismiss_overlay_strict(
    page,
    category=None,
    batch_id=None,
    *,
    label: str = "",
    is_present: Callable[..., bool] | None = None,
    extra_close_selectors: Sequence[str] = (),
    click_modal_backdrop: bool = False,
) -> None:
    """Одна цепочка без пауз и ретраев: снаружи → Escape → × → (backdrop).

    Если оверлей остался — OverlayNotDismissedError.
    """
    present = is_present or _likely_overlay_present
    if not present(page):
        return

    _user_lvl = "info" if batch_id else "silent"
    prefix = f"{label}: " if label else ""

    if _click_safe_free_field(page):
        write_log_entry(
            batch_id, category,
            f"{prefix}Закрываю оверлей — клик в свободную область.",
            level=_user_lvl,
        )
        if not present(page):
            write_log_entry(
                batch_id, category,
                f"{prefix}Оверлей закрыт.",
                level=_user_lvl,
            )
            return

    write_log_entry(
        batch_id, category,
        f"{prefix}Закрываю оверлей — Escape.",
        level=_user_lvl,
    )
    _try_escape(page)
    if not present(page):
        write_log_entry(
            batch_id, category,
            f"{prefix}Оверлей закрыт.",
            level=_user_lvl,
        )
        return

    closed = _try_generic_close(page) or _try_close_selectors(page, extra_close_selectors)
    if closed:
        write_log_entry(
            batch_id, category,
            f"{prefix}Закрываю оверлей — кнопка закрытия.",
            level=_user_lvl,
        )
        if not present(page):
            write_log_entry(
                batch_id, category,
                f"{prefix}Оверлей закрыт.",
                level=_user_lvl,
            )
            return

    if click_modal_backdrop:
        try:
            page.locator("[data-testid='modal-overlay']").first.click(
                force=True, timeout=2_000,
            )
        except Exception:
            pass
        if not present(page):
            write_log_entry(
                batch_id, category,
                f"{prefix}Оверлей закрыт.",
                level=_user_lvl,
            )
            return

    raise OverlayNotDismissedError(
        f"{prefix}Не удалось закрыть оверлей — все действия исчерпаны."
    )


def _click_safe_free_field(page, inset: int = 24) -> bool:
    try:
        pt = page.evaluate(_SAFE_FIELD_JS, inset)
    except Exception:
        return False
    if not pt:
        return False
    try:
        page.mouse.click(pt["x"], pt["y"])
        return True
    except Exception:
        return False


def _try_escape(page) -> None:
    try:
        page.keyboard.press("Escape")
    except Exception:
        pass


def _try_generic_close(page) -> bool:
    for sel in (
        "button[aria-label*='Закрыть']",
        "button[aria-label*='закрыть']",
        "button[aria-label*='Close']",
    ):
        try:
            btn = page.locator(sel).first
            if btn.is_visible(timeout=200):
                btn.click(timeout=2_000)
                return True
        except Exception:
            pass
    return False


def _likely_overlay_present(page) -> bool:
    """True если на странице похоже на модал/оверлей (не чистый дашборд)."""
    for sel in (
        "[role='dialog']",
        "[role='alertdialog']",
        "[aria-modal='true']",
        "[class*='modal']",
        "[class*='Modal']",
        "[class*='overlay']",
        "[class*='Overlay']",
        "[class*='popup']",
        "[class*='Popup']",
        "[class*='drawer']",
        "[class*='Drawer']",
    ):
        try:
            if page.locator(sel).first.is_visible(timeout=150):
                return True
        except Exception:
            pass
    return False


def dismiss_dzen_hint(
    page,
    category=None,
    batch_id=None,
    *,
    label: str = "Дзен",
    phase: int = 0,
    force: bool = False,
) -> None:
    """Закрывает helper-tooltip хинт Дзена. Без click-outside (ломает меню «+»).

    phase/force — совместимость с safe_click; hint не использует click-outside.
    """
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
            f"{prefix}Закрываю оверлей — кнопка хинта (попытка {_attempt + 1}).",
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
                "[dzen] hint click failed, retrying.",
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
            "[dzen] хинт всё ещё виден после клика — повтор.",
            level="silent",
        )

    if hint_was_seen:
        write_log_entry(
            batch_id, category,
            f"{prefix}Оверлей не закрылся за 3 попытки.",
            level=_warn_lvl,
        )


def dismiss_click_outside(
    page,
    category=None,
    batch_id=None,
    *,
    label: str = "",
    phase: int = 0,
    force: bool = False,
) -> None:
    """Rutube/VK: strict dismiss (phase/force ignored, kept for safe_click compat)."""
    del phase, force
    dismiss_overlay_strict(page, category, batch_id, label=label)


def safe_click(
    locator,
    page,
    whitelist: Sequence[WhitelistEntry],
    dismiss_unknown: DismissUnknown,
    *,
    batch_id=None,
    category=None,
    label: str = "",
    timeout_ms: int = 30_000,
    max_attempts: int = 3,
    click_kwargs: dict[str, Any] | None = None,
    js_fallback: bool = False,
) -> None:
    """handle_popups → dismiss → click; короткий timeout, без 30-с Playwright-retry."""
    opts = dict(click_kwargs or {})
    _click_timeout_ms = min(timeout_ms, 2_000)

    last_err: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        def _dismiss(page, category, batch_id):
            dismiss_unknown(page, category, batch_id, label=label)

        handle_popups(page, whitelist, _dismiss, batch_id, category)
        try:
            locator.click(timeout=_click_timeout_ms, **opts)
            return
        except Exception as exc:
            last_err = exc
            write_log_entry(
                batch_id, category,
                f"{label}: Клик заблокирован (попытка {attempt}/{max_attempts}).",
                level="info" if batch_id else "silent",
            )
            if js_fallback and attempt == max_attempts:
                try:
                    locator.evaluate("el => el.click()")
                    return
                except Exception as js_exc:
                    last_err = js_exc
    if last_err is not None:
        raise last_err


_PREVIEW_POLL_MS = 200


def poll_wait_tick(
    page,
    batch_id=None,
    platform: str | None = None,
    poll_ms: int = _PREVIEW_POLL_MS,
) -> None:
    """Пауза в wait-цикле: inline-кадр при сбое CDP, иначе только sleep."""
    _maybe_inline_publish_preview(page, batch_id, platform)
    page.wait_for_timeout(poll_ms)


def poll_until(
    page,
    predicate: Callable[[], bool],
    timeout_ms: int,
    *,
    batch_id=None,
    platform: str | None = None,
    poll_ms: int = _PREVIEW_POLL_MS,
    on_poll: Callable[[], None] | None = None,
) -> bool:
    """Ожидает predicate; между итерациями — poll_wait_tick (200 ms)."""
    deadline = _time.monotonic() + timeout_ms / 1000
    while _time.monotonic() < deadline:
        if on_poll is not None:
            on_poll()
        if predicate():
            return True
        poll_wait_tick(page, batch_id, platform, poll_ms)
    if on_poll is not None:
        on_poll()
    return predicate()


def _maybe_inline_publish_preview(page, batch_id, platform: str | None) -> None:
    if not batch_id or not platform:
        return
    from services.publish_preview_capture import needs_inline_preview

    if not needs_inline_preview(batch_id):
        return
    try:
        from services.browser_registry import get_browser

        img = page.screenshot(type="jpeg", quality=65)
        get_browser(platform).push_frame_for_batch(batch_id, img)
    except Exception:
        pass
