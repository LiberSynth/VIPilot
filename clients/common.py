"""
Общие утилиты Playwright-kлиентов публикации (dzen, rutube, vkvideo).

Единый контракт: whitelist (captcha/confirm/шаг) → ждём целевой элемент →
если не видим/перекрыт и есть overlay по классовым признакам → dismiss.
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

# Затемняющий scrim: fixed/absolute слой ≥60% viewport с полупрозрачным фоном.
_OVERLAY_SCRIM_JS = """
() => {
  const vw = window.innerWidth;
  const vh = window.innerHeight;
  const minW = vw * 0.55;
  const minH = vh * 0.55;
  for (const el of document.body.querySelectorAll('*')) {
    const st = window.getComputedStyle(el);
    if (st.display === 'none' || st.visibility === 'hidden') continue;
    if (st.pointerEvents === 'none') continue;
    const pos = st.position;
    if (pos !== 'fixed' && pos !== 'absolute') continue;
    const rect = el.getBoundingClientRect();
    if (rect.width < minW || rect.height < minH) continue;
    const bg = st.backgroundColor;
    if (!bg || bg === 'transparent' || bg === 'rgba(0, 0, 0, 0)') continue;
    const m = bg.match(/rgba?\\((\\d+),\\s*(\\d+),\\s*(\\d+)(?:,\\s*([\\d.]+))?\\)/);
    if (!m) continue;
    const a = m[4] !== undefined ? parseFloat(m[4]) : 1;
    if (a < 0.12) continue;
    const rv = parseInt(m[1], 10);
    const gv = parseInt(m[2], 10);
    const bv = parseInt(m[3], 10);
    if ((rv + gv + bv) / 3 > 120) continue;
    const z = parseInt(st.zIndex, 10);
    if (Number.isFinite(z) && z < 1) continue;
    return true;
  }
  return false;
}
"""


class OverlayNotDismissedError(RuntimeError):
    """Оверлей остался на экране после полной цепочки закрывающих действий."""


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

_ELEMENT_OBSTRUCTED_JS = """(el) => {
    const st = window.getComputedStyle(el);
    if (st.visibility === 'hidden' || st.display === 'none') return false;
    const r = el.getBoundingClientRect();
    if (r.width < 8 || r.height < 8) return false;
    const cx = r.left + r.width / 2;
    const cy = r.top + r.height / 2;
    const top = document.elementFromPoint(cx, cy);
    if (!top) return true;
    return !(top === el || el.contains(top));
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


def element_obstructed(locator) -> bool:
    """Центр элемента перекрыт другим UI (без учёта disabled)."""
    try:
        if not locator.is_visible(timeout=200):
            return False
        return bool(locator.evaluate(_ELEMENT_OBSTRUCTED_JS))
    except Exception:
        return False


def publish_target_needs_dismiss(target) -> bool:
    """Целевой элемент не найден, не виден или перекрыт overlay."""
    if target is None:
        return True
    try:
        if not target.is_visible(timeout=150):
            return True
    except Exception:
        return True
    return element_obstructed(target)


def noop_dismiss_unknown(*_args, **_kwargs) -> None:
    """Заглушка для handle_popups: dismiss только через try_dismiss_publish_overlay."""


def handle_popups(
    page,
    whitelist: Sequence[WhitelistEntry],
    dismiss_unknown: DismissUnknown,
    batch_id=None,
    category=None,
    *,
    allow_dismiss: bool = False,
) -> None:
    """
    whitelist → handler; иначе noop (dismiss — только target-first, см. try_dismiss).
    allow_dismiss=True — устаревший путь, вызывает dismiss_unknown без target.
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
        if handle is not None:
            handle(page, category, batch_id)
        return
    if allow_dismiss:
        dismiss_unknown(page, category, batch_id)


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


_DISMISS_COOLDOWN_SEC = 2.0
_last_dismiss_at: dict[int, float] = {}

# Блокирующий слой: dialog/modal/scrim (не голый backdrop без контента).
_STRONG_OVERLAY_LAYER_SELECTORS: tuple[str, ...] = (
    "[role='dialog']",
    "[role='alertdialog']",
    "[aria-modal='true']",
    "[class*='ModalOverlay']",
    "[class*='modalOverlay']",
    "[class*='ModalLayout']",
    "[class*='PopoutRoot']",
    "[class*='CoachMark']",
    "[class*='coachmark']",
)

_OVERLAY_BACKDROP_SELECTORS: tuple[str, ...] = (
    "[data-testid='modal-overlay']",
    "[class*='ModalOverlay']",
    "[class*='modalOverlay']",
    "[class*='Backdrop']",
    "[class*='backdrop']",
)

_OVERLAY_CONTENT_SELECTORS: tuple[str, ...] = (
    "[role='dialog']",
    "[role='tooltip']",
    "[class*='modal__rootElement']",
    "[class*='ModalRoot']",
    "[class*='modalRoot']",
    "[class*='Popover']",
    "[class*='popover']",
    "[class*='Tooltip']",
    "[class*='tooltip']",
    "[aria-modal='true']",
)

_OVERLAY_CLOSE_SELECTORS: tuple[str, ...] = (
    "[class*='helper-tooltip__closeButton']",
    "[class*='modal__rootElement'] button[class*='close']",
    "[class*='modal__rootElement'] [class*='Close']",
    "[class*='modal__rootElement'] button[aria-label*='lose']",
    "[class*='modal__rootElement'] button[aria-label*='закр']",
    "[class*='modal__rootElement'] button[aria-label*='Закр']",
    "[class*='popover'] button[class*='close']",
    "[class*='Popover'] button[class*='close']",
    "[class*='tooltip'] button[class*='close']",
    "[class*='Tooltip'] button[class*='close']",
    "[class*='tooltip'] [class*='closeIcon']",
    "[class*='Tooltip'] [class*='closeIcon']",
    "[class*='tour'] button[class*='close']",
    "[class*='Tour'] button[class*='close']",
    "[class*='popup'] button[class*='close']",
    "[class*='modal'] button[class*='close']",
    "[class*='closeButton']",
    "[class*='CloseButton']",
    "[class*='closeIcon']",
    "[class*='CloseIcon']",
    "button[aria-label*='Закрыть']",
    "button[aria-label*='закрыть']",
    "button[aria-label*='Close']",
    "[class*='toast'] button[class*='close']",
    "[class*='toast'] [class*='closeButton']",
    "[class*='notification'] button[class*='close']",
    "[class*='notification'] [class*='closeButton']",
    "[class*='snackbar'] button[class*='close']",
    "[class*='snackbar'] [class*='closeButton']",
    "[role='alert'] button",
    "[role='alert'] [class*='close']",
    "[role='alertdialog'] button[class*='close']",
    "[class*='modal'] button:has-text('Понятно')",
    "[class*='modal'] button:has-text('Не сейчас')",
    "[class*='modal'] button:has-text('Пропустить')",
    "[class*='modal'] button:has-text('Позже')",
    "[class*='popover'] button:has-text('Пропустить')",
    "[class*='tooltip'] button:has-text('Пропустить')",
)


def _overlay_scrim_visible(page) -> bool:
    try:
        return bool(page.evaluate(_OVERLAY_SCRIM_JS))
    except Exception:
        return False


def _modal_backdrop_with_content_visible(page) -> bool:
    """Backdrop (dzen modal-overlay и т.п.) только если поверх него есть modal/dialog."""
    for backdrop in _OVERLAY_BACKDROP_SELECTORS:
        try:
            if not page.locator(backdrop).first.is_visible(timeout=150):
                continue
        except Exception:
            continue
        for content in _OVERLAY_CONTENT_SELECTORS:
            try:
                if page.locator(content).first.is_visible(timeout=150):
                    return True
            except Exception:
                pass
    return False


def _blocking_popover_visible(page) -> bool:
    """Крупная popover/coachmark-карточка (онбординг), не layout-tooltip."""
    for sel in (
        "[class*='Popover']",
        "[class*='popover']",
        "[class*='CoachMark']",
        "[class*='coachmark']",
        "[class*='Tour']",
        "[class*='tour']",
        "[class*='Onboarding']",
        "[class*='onboarding']",
    ):
        try:
            loc = page.locator(sel).first
            if not loc.is_visible(timeout=150):
                continue
            box = loc.bounding_box()
            if box and box.get("width", 0) >= 180 and box.get("height", 0) >= 60:
                return True
        except Exception:
            pass
    return False


def publish_overlay_visible(page) -> bool:
    """Блокирующий overlay: scrim, modal/dialog, backdrop+контент или popover-карточка."""
    if _overlay_scrim_visible(page):
        return True
    if _blocking_popover_visible(page):
        return True
    if _modal_backdrop_with_content_visible(page):
        return True
    for sel in _STRONG_OVERLAY_LAYER_SELECTORS:
        try:
            if page.locator(sel).first.is_visible(timeout=150):
                return True
        except Exception:
            pass
    return False


def whitelisted_publish_ui(page, whitelist: Sequence[WhitelistEntry]) -> bool:
    for _name, detect, _handle in whitelist:
        try:
            if detect(page):
                return True
        except _DETECT_BUG_EXCEPTIONS:
            raise
        except Exception:
            pass
    return False


def publish_overlay_is_garbage(page, whitelist: Sequence[WhitelistEntry]) -> bool:
    """Overlay есть и не входит в whitelist штатного UI."""
    if not publish_overlay_visible(page):
        return False
    return not whitelisted_publish_ui(page, whitelist)


def _step_click_outside_overlay_backdrop(page) -> bool:
    for backdrop in _OVERLAY_BACKDROP_SELECTORS:
        for content in _OVERLAY_CONTENT_SELECTORS:
            if click_outside_modal_boundary(page, backdrop, content):
                return True
        if click_outside_modal_boundary(page, backdrop, ""):
            return True
    return False


def _step_click_overlay_layer(page) -> bool:
    for sel in ("[role='alert']", "[role='alertdialog']", "[class*='toast']", "[class*='notification']"):
        try:
            loc = page.locator(sel).first
            if loc.is_visible(timeout=150):
                loc.click(timeout=2_000)
                return True
        except Exception:
            pass
    return False


def _step_click_close_buttons(page) -> bool:
    for sel in _OVERLAY_CLOSE_SELECTORS:
        try:
            btn = page.locator(sel).first
            if btn.is_visible(timeout=150):
                btn.click(timeout=2_000)
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
        suffix = "" if performed else " (не найдено)"
        write_log_entry(
            batch_id, category,
            f"{prefix}{log_msg}{suffix}",
            level=user_lvl,
        )
        if performed and present(page):
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
) -> None:
    """Единая цепочка закрытия overlay; каждый шаг логируется.

    Порядок: backdrop → свободная область → alert/toast → Escape → ×.
    Если оверлей остался — OverlayNotDismissedError.
    """
    present = is_present or publish_overlay_visible
    if not present(page):
        return

    _user_lvl = "info" if batch_id else "silent"
    prefix = f"{label}: " if label else ""

    steps: list[DismissStep] = [
        ("сделан клик за границей окна", _step_click_outside_overlay_backdrop),
        ("сделан клик в свободную область", _click_safe_free_field),
        ("сделан клик по уведомлению", _step_click_overlay_layer),
        ("нажат Escape", _step_escape),
        ("сделан клик по кнопке закрытия", _step_click_close_buttons),
    ]
    _run_dismiss_steps(page, present, batch_id, category, prefix, _user_lvl, steps)

    if present(page):
        _wait_overlay_gone(page, present)
    if not present(page):
        return
    raise OverlayNotDismissedError(
        f"{prefix}Не удалось закрыть оверлей — все действия исчерпаны."
    )


def try_dismiss_publish_overlay(
    page,
    whitelist: Sequence[WhitelistEntry],
    batch_id=None,
    category=None,
    *,
    target=None,
    label: str = "",
    error_factory: type[Exception] | None = None,
    raise_on_failure: bool = False,
) -> bool:
    """Target-first: dismiss только если цель не готова и overlay виден по классовым признакам."""
    del whitelist  # whitelist не блокирует dismiss при перекрытой цели
    if not publish_target_needs_dismiss(target):
        return False
    if target is not None and element_center_clickable(target):
        return False
    if not publish_overlay_visible(page):
        return False
    page_id = id(page)
    now = _time.monotonic()
    if now - _last_dismiss_at.get(page_id, 0.0) < _DISMISS_COOLDOWN_SEC:
        return False
    _last_dismiss_at[page_id] = now
    _user_lvl = "info" if batch_id else "silent"
    prefix = f"{label}: " if label else ""
    write_log_entry(
        batch_id, category,
        f"{prefix}Закрываю мусорный overlay.",
        level=_user_lvl,
    )
    try:
        dismiss_overlay_strict(
            page, category, batch_id, label=label,
            is_present=publish_overlay_visible,
        )
    except OverlayNotDismissedError as exc:
        if raise_on_failure and error_factory is not None:
            raise error_factory(str(exc)) from exc
        if raise_on_failure:
            raise
        return False
    if target is not None and element_center_clickable(target):
        return True
    if publish_overlay_visible(page):
        if raise_on_failure and error_factory is not None:
            raise error_factory(
                f"{prefix}Не удалось закрыть оверлей — все действия исчерпаны."
            )
        return False
    return True


def dismiss_publish_overlay(
    page,
    whitelist: Sequence[WhitelistEntry],
    batch_id=None,
    category=None,
    *,
    label: str = "",
    error_factory: type[Exception] | None = None,
    target=None,
    blocked_locator=None,
) -> None:
    """Обёртка try_dismiss с raise_on_failure (blocked_locator → target)."""
    tgt = target if target is not None else blocked_locator
    try_dismiss_publish_overlay(
        page, whitelist, batch_id, category,
        target=tgt, label=label, error_factory=error_factory,
        raise_on_failure=True,
    )


def wait_for_publish_target(
    page,
    *,
    find_target: Callable[[], Any],
    is_ready: Callable[[Any], bool] | None = None,
    whitelist: Sequence[WhitelistEntry],
    batch_id=None,
    category=None,
    platform: str | None = None,
    label: str = "",
    timeout_ms: int = 180_000,
    log_every_sec: float = 8.0,
    status_message: Callable[[Any], str] | None = None,
    before_poll: Callable[..., None] | None = None,
    error_factory: type[Exception] | None = None,
    timeout_message: str = "",
) -> Any:
    """Единое ожидание цели: whitelist → target-first dismiss → poll."""
    found: list[Any] = [None]
    last_log_at = 0.0

    def _ready_fn(target) -> bool:
        if is_ready is not None:
            return is_ready(target)
        return target is not None and element_center_clickable(target)

    def _on_poll() -> None:
        nonlocal last_log_at
        if before_poll is not None:
            before_poll()
        handle_popups(page, whitelist, noop_dismiss_unknown, batch_id, category)
        target = find_target()
        try_dismiss_publish_overlay(
            page, whitelist, batch_id, category,
            target=target, label=label,
        )
        if log_every_sec <= 0 or status_message is None:
            return
        now = _time.monotonic()
        if now - last_log_at < log_every_sec:
            return
        msg = status_message(find_target())
        if msg:
            prefix = f"{label}: " if label else ""
            write_log_entry(batch_id, category, f"{prefix}{msg}")
        last_log_at = now

    def _predicate() -> bool:
        target = find_target()
        if _ready_fn(target):
            found[0] = target
            return True
        return False

    if poll_until(
        page, _predicate, timeout_ms,
        batch_id=batch_id, platform=platform, on_poll=_on_poll,
    ):
        return found[0]
    if error_factory is not None and timeout_message:
        raise error_factory(timeout_message)
    return None


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
        handle_popups(page, whitelist, noop_dismiss_unknown, batch_id, category)
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
            if not element_center_clickable(locator):
                dismiss_unknown(
                    page, category, batch_id, label=label, target=locator,
                    raise_on_failure=False,
                )
                try_dismiss_publish_overlay(
                    page, whitelist, batch_id, category,
                    target=locator, label=label,
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
