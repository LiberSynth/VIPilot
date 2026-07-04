"""
Общие утилиты Playwright-клиентов публикации (dzen, rutube, vkvideo).

Единый контракт: handle_popups(whitelist) -> dismiss_unknown.
Платформенное (whitelist, is_present, extra_close_selectors, extra_steps)
передаётся вызывающим кодом; common не импортирует прикладные модули.
"""

from __future__ import annotations

import time as _time
from collections.abc import Callable, Sequence
from typing import Any

from log import write_log_entry

# (имя, detect(page)->bool, handle(page, category, batch_id)|None)
WhitelistEntry = tuple[str, Callable[..., bool], Callable[..., None] | None]

DismissUnknown = Callable[..., None]

DismissStep = tuple[str, Callable[..., bool]]

_DETECT_BUG_EXCEPTIONS = (
    NameError,
    TypeError,
    AttributeError,
    ImportError,
    SyntaxError,
    UnboundLocalError,
    RecursionError,
)

_DISMISS_SETTLE_MS = 600
_DISMISS_POLL_MS = 50

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

_OUTSIDE_MODAL_JS = """
(args) => {
  const [overlaySel, modalSel, inset] = args;
  const overlay = document.querySelector(overlaySel);
  if (!overlay) return null;
  const oStyle = window.getComputedStyle(overlay);
  if (oStyle.display === 'none' || oStyle.visibility === 'hidden') return null;
  const or = overlay.getBoundingClientRect();
  if (or.width <= 0 || or.height <= 0) return null;
  const modal = modalSel ? document.querySelector(modalSel) : null;
  const mr = modal ? modal.getBoundingClientRect() : null;
  const insideModal = (x, y) => {
    if (!mr || mr.width <= 0 || mr.height <= 0) return false;
    return x >= mr.left && x <= mr.right && y >= mr.top && y <= mr.bottom;
  };
  const onOverlay = (el) => {
    if (!el) return false;
    return el === overlay || overlay.contains(el);
  };
  const w = window.innerWidth;
  const h = window.innerHeight;
  const points = [
    [inset, inset],
    [w - inset, inset],
    [inset, h - inset],
    [w - inset, h - inset],
  ];
  if (mr) {
    points.push(
      [Math.round((or.left + mr.left) / 2), Math.round(or.top + inset)],
      [Math.round((mr.right + or.right) / 2), Math.round(or.top + inset)],
      [Math.round((mr.left + mr.right) / 2), Math.round(Math.max(or.top + inset, mr.top - inset))],
      [Math.round((mr.left + mr.right) / 2), Math.round(Math.min(or.bottom - inset, mr.bottom + inset))],
      [Math.round(Math.max(or.left + inset, mr.left - inset * 2)), Math.round((mr.top + mr.bottom) / 2)],
      [Math.round(Math.min(or.right - inset, mr.right + inset * 2)), Math.round((mr.top + mr.bottom) / 2)],
    );
  }
  for (const [x, y] of points) {
    if (insideModal(x, y)) continue;
    const el = document.elementFromPoint(x, y);
    if (onOverlay(el)) return { x, y };
  }
  return null;
}
"""


class OverlayNotDismissedError(RuntimeError):
    """Оверлей остался на экране после полной цепочки закрывающих действий."""


def _visible(page, locator, timeout_ms: int = 400) -> bool:
    try:
        return locator.first.is_visible(timeout=timeout_ms)
    except Exception:
        return False


_ELEMENT_CENTER_HIT_JS = """(el) => {
    if (el.disabled) return false;
    if (el.getAttribute('aria-disabled') === 'true') return false;
    const st = window.getComputedStyle(el);
    if (st.pointerEvents === 'none') return false;
    if (st.visibility === 'hidden' || st.display === 'none') return false;
    const r = el.getBoundingClientRect();
    if (r.width < 8 || r.height < 8) return false;
    const cx = r.left + r.width / 2;
    const cy = r.top + r.height / 2;
    const top = document.elementFromPoint(cx, cy);
    return !!(top && (top === el || el.contains(top)));
}"""


def element_center_clickable(locator) -> bool:
    """Центр элемента не перекрыт другим UI (elementFromPoint)."""
    try:
        if not locator.is_visible(timeout=200):
            return False
        return bool(locator.evaluate(_ELEMENT_CENTER_HIT_JS))
    except Exception:
        return False


def element_click_blocked(locator) -> bool:
    """Элемент виден, но центр перекрыт — признак мусора поверх whitelisted UI."""
    try:
        if not locator.is_visible(timeout=200):
            return False
        return not bool(locator.evaluate(_ELEMENT_CENTER_HIT_JS))
    except Exception:
        return False


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
    whitelist -> иначе dismiss_unknown.
    handle=None — элемент известен, не закрывать.
    allow_dismiss=False — только whitelist (ожидание целевого UI, без dismiss).
    """
    for name, detect, handle in whitelist:
        try:
            if not detect(page):
                continue
        except _DETECT_BUG_EXCEPTIONS:
            raise
        except Exception as exc:
            write_log_entry(
                batch_id, category,
                f"whitelist detect {name!r}: {exc}",
                level="silent",
            )
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


def _step_escape(page) -> bool:
    _try_escape(page)
    return True


def click_outside_modal_boundary(
    page,
    overlay_selector: str,
    modal_selector: str = "",
    *,
    inset: int = 16,
) -> bool:
    """Клик по затемнённому backdrop вне прямоугольника modal_selector."""
    try:
        pt = page.evaluate(
            _OUTSIDE_MODAL_JS,
            [overlay_selector, modal_selector or None, inset],
        )
    except Exception:
        return False
    if not pt:
        return False
    try:
        page.mouse.click(pt["x"], pt["y"])
        return True
    except Exception:
        return False


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
        "[role='alert']",
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


def _wait_overlay_gone(page, present: Callable[..., bool], timeout_ms: int = _DISMISS_SETTLE_MS) -> bool:
    """Ждёт исчезновения оверлея после закрывающего действия (анимация)."""
    deadline = _time.monotonic() + timeout_ms / 1000
    while _time.monotonic() < deadline:
        if not present(page):
            return True
        page.wait_for_timeout(_DISMISS_POLL_MS)
    return not present(page)


def _run_dismiss_steps(
    page,
    present: Callable[..., bool],
    batch_id,
    category,
    prefix: str,
    user_lvl: str,
    steps: Sequence[DismissStep],
) -> None:
    for log_msg, action in steps:
        if not present(page):
            return
        try:
            performed = action(page)
        except Exception:
            performed = False
        if performed:
            write_log_entry(
                batch_id, category,
                f"{prefix}{log_msg}",
                level=user_lvl,
            )
            if present(page):
                _wait_overlay_gone(page, present)
        if not present(page):
            write_log_entry(
                batch_id, category,
                f"{prefix}Оверлей закрыт.",
                level=user_lvl,
            )
            return


def dismiss_overlay_strict(
    page,
    category=None,
    batch_id=None,
    *,
    label: str = "",
    is_present: Callable[..., bool] | None = None,
    extra_close_selectors: Sequence[str] = (),
    extra_steps: Sequence[DismissStep] = (),
) -> None:
    """Одна цепочка закрывающих действий; каждый шаг логируется после выполнения.

    После каждого шага — короткое ожидание исчезновения оверлея (анимация).
    Базовый порядок: свободная область -> extra_steps -> Escape -> x.
    Если оверлей остался — OverlayNotDismissedError.
    """
    present = is_present or _likely_overlay_present
    if not present(page):
        return

    _user_lvl = "info" if batch_id else "silent"
    prefix = f"{label}: " if label else ""

    def _try_close(page) -> bool:
        return _try_generic_close(page) or _try_close_selectors(page, extra_close_selectors)

    steps: list[DismissStep] = [
        ("сделан клик в свободную область", _click_safe_free_field),
        *extra_steps,
        ("нажат Escape", _step_escape),
        ("сделан клик по кнопке закрытия", _try_close),
    ]
    _run_dismiss_steps(page, present, batch_id, category, prefix, _user_lvl, steps)

    if present(page):
        _wait_overlay_gone(page, present)
    if not present(page):
        return
    raise OverlayNotDismissedError(
        f"{prefix}Не удалось закрыть оверлей — все действия исчерпаны."
    )


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
    """handle_popups -> dismiss -> click; короткий timeout, без 30-с Playwright-retry."""
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
_SHUTDOWN_WAIT_CHUNK_MS = 50


def _raise_if_shutting_down() -> None:
    from common.exceptions import ShutdownRequested
    from common.shutdown import is_shutting_down

    if is_shutting_down():
        raise ShutdownRequested()


def _interruptible_page_wait(page, timeout_ms: int) -> None:
    from common.exceptions import ShutdownRequested
    from common.shutdown import is_playwright_shutdown_error, is_shutting_down

    remaining = timeout_ms
    while remaining > 0:
        _raise_if_shutting_down()
        step = min(_SHUTDOWN_WAIT_CHUNK_MS, remaining)
        try:
            page.wait_for_timeout(step)
        except Exception as exc:
            if is_shutting_down() or is_playwright_shutdown_error(exc):
                raise ShutdownRequested() from exc
            raise
        remaining -= step


def poll_wait_tick(
    page,
    batch_id=None,
    platform: str | None = None,
    poll_ms: int = _PREVIEW_POLL_MS,
) -> None:
    """Пауза в wait-цикле: inline-кадр при сбое CDP, иначе только sleep."""
    _raise_if_shutting_down()
    _maybe_inline_publish_preview(page, batch_id, platform)
    _interruptible_page_wait(page, poll_ms)


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
        _raise_if_shutting_down()
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
