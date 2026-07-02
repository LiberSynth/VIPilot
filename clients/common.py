"""
Общие утилиты Playwright-клиентов публикации.

Паттерн handle_popups + whitelist — из отлаженного clients/dzen.py.
dismiss_click_outside — только для Rutube/VK (Dzen использует свой hint-only dismiss).
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

_DISMISS_STATE_KEY = "_vipilot_dismiss_state"
_DISMISS_COOLDOWN_S = 2.0

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


def _dismiss_state(page) -> dict:
    st = getattr(page, _DISMISS_STATE_KEY, None)
    if st is None:
        st = {"outside_used": False, "last_dismiss_at": 0.0}
        setattr(page, _DISMISS_STATE_KEY, st)
    return st


def _reset_dismiss_state(page) -> None:
    st = getattr(page, _DISMISS_STATE_KEY, None)
    if st is not None:
        st["outside_used"] = False


def _overlay_cleared(page, *, force: bool) -> bool:
    return force or not _likely_overlay_present(page)


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


def dismiss_click_outside(
    page,
    category=None,
    batch_id=None,
    *,
    label: str = "",
    phase: int = 0,
    force: bool = False,
) -> None:
    """Закрытие неизвестного оверлея для Rutube/VK.

    phase 0: один клик снаружи → если оверлей остался → Escape → ×
    phase 1: Escape → × (без клика снаружи)
    phase 2+: × → Escape

    Клик снаружи не повторяется, пока оверлей не закроется (state на page).
    force=True — после заблокированного клика, без cooldown.
    """
    if not force and not _likely_overlay_present(page):
        _reset_dismiss_state(page)
        return

    _user_lvl = "info" if batch_id else "silent"
    prefix = f"{label}: " if label else ""
    st = _dismiss_state(page)

    if phase >= 1:
        st["outside_used"] = True

    if not force:
        since = _time.monotonic() - st["last_dismiss_at"]
        if since < _DISMISS_COOLDOWN_S:
            return
    st["last_dismiss_at"] = _time.monotonic()

    if phase <= 0 and not st["outside_used"]:
        if _click_safe_free_field(page):
            write_log_entry(
                batch_id, category,
                f"{prefix}Закрываю оверлей — клик в свободную область.",
                level=_user_lvl,
            )
            page.wait_for_timeout(200)
        st["outside_used"] = True
        if _overlay_cleared(page, force=force):
            _reset_dismiss_state(page)
            return

    if phase <= 1:
        write_log_entry(
            batch_id, category,
            f"{prefix}Закрываю оверлей — Escape.",
            level=_user_lvl,
        )
        _try_escape(page)
        page.wait_for_timeout(200)
        if _overlay_cleared(page, force=force):
            _reset_dismiss_state(page)
            return
        if _try_generic_close(page):
            write_log_entry(
                batch_id, category,
                f"{prefix}Закрываю оверлей — кнопка закрытия.",
                level=_user_lvl,
            )
            page.wait_for_timeout(200)
        if _overlay_cleared(page, force=force):
            _reset_dismiss_state(page)
        return

    if _try_generic_close(page):
        write_log_entry(
            batch_id, category,
            f"{prefix}Закрываю оверлей — кнопка закрытия.",
            level=_user_lvl,
        )
        page.wait_for_timeout(200)
    else:
        write_log_entry(
            batch_id, category,
            f"{prefix}Закрываю оверлей — Escape.",
            level=_user_lvl,
        )
        _try_escape(page)
        page.wait_for_timeout(200)
    if _overlay_cleared(page, force=force):
        _reset_dismiss_state(page)


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
    max_attempts: int = 5,
    click_kwargs: dict[str, Any] | None = None,
    js_fallback: bool = False,
) -> None:
    """handle_popups → click; при блокировке эскалирует dismiss, не повторяет одно действие."""
    opts = dict(click_kwargs or {})
    _blocked_timeout_ms = min(timeout_ms, 2_000)

    last_err: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        _phase = min(attempt - 1, 2)
        _force = attempt > 1

        def _dismiss(page, category, batch_id, *, p=_phase, f=_force):
            if f:
                _reset_dismiss_state(page)
            dismiss_unknown(
                page, category, batch_id, label=label, phase=p, force=f,
            )

        handle_popups(page, whitelist, _dismiss, batch_id, category)
        _click_timeout = (
            timeout_ms if attempt == max_attempts else _blocked_timeout_ms
        )
        try:
            locator.click(timeout=_click_timeout, **opts)
            return
        except Exception as exc:
            last_err = exc
            write_log_entry(
                batch_id, category,
                f"{label}: Клик заблокирован (попытка {attempt}/{max_attempts}).",
                level="warn" if batch_id else "silent",
            )
            if js_fallback and attempt == max_attempts:
                try:
                    locator.evaluate("el => el.click()")
                    return
                except Exception as js_exc:
                    last_err = js_exc
            page.wait_for_timeout(300)
    if last_err is not None:
        raise last_err
