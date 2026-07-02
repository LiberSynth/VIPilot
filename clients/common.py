"""
Общие утилиты Playwright-клиентов публикации (dzen, rutube, vkvideo).

- проверка сессии / экрана входа;
- закрытие неизвестных оверлеев (метод исключения + whitelist per-client);
- safe_click с повторными попытками.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from log import write_log_entry

_SESSION_MSG = (
    "Сессия истекла — авторизуйтесь снова в браузере (вкладка «Публикация»)"
)

_DZEN_STUDIO_SELECTORS = (
    "[class*='author-studio-header'], "
    "[data-testid='add-publication-button'], "
    "[class*='author-studio-header__addButton'], "
    "[class*='addButton']"
)

# (имя, detect(page)->bool, handle(page, category, batch_id)|None)
WhitelistEntry = tuple[str, Callable[..., bool], Callable[..., None] | None]

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


def _rutube_login_ui(page) -> bool:
    if not _visible(page, page.get_by_text("Вход", exact=True)):
        return False
    if _visible(page, page.get_by_placeholder("Телефон или почта"), 300):
        return True
    if _visible(page, page.locator("input[placeholder*='Телефон']"), 300):
        return True
    if _visible(page, page.locator("input[placeholder*='почта']"), 300):
        return True
    return _visible(page, page.get_by_text("Телефон или почта", exact=False), 300)


def _dzen_login_ui(page) -> bool:
    if _visible(page, page.get_by_text("Войдите", exact=False), 300):
        return True
    if _visible(page, page.get_by_text("Войти", exact=False), 300):
        if _visible(page, page.locator("input[name='login'], input[type='tel']"), 300):
            return True
    if _visible(page, page.get_by_text("Yandex ID", exact=False), 300):
        if _visible(page, page.locator("input[type='password'], input[name='passwd']"), 300):
            return True
    return False


def _vkvideo_login_ui(page) -> bool:
    markers = (
        "Вход ВКонтакте",
        "Вход в VK",
        "Sign in to VK",
        "Войти в аккаунт",
    )
    for text in markers:
        if _visible(page, page.get_by_text(text, exact=False), 300):
            return True
    if _visible(page, page.get_by_placeholder("Телефон", exact=False), 300):
        return True
    if _visible(page, page.locator("input[name='login'], input[type='tel']"), 300):
        if _visible(page, page.get_by_text("Войти", exact=False), 300):
            return True
    return False


def _dzen_studio_markers_visible(page) -> bool:
    return _visible(page, page.locator(_DZEN_STUDIO_SELECTORS), 400)


def _dzen_public_channel_view(page) -> bool:
    if _visible(page, page.get_by_role("button", name="Подписаться"), 300):
        return True
    if _visible(page, page.get_by_role("button", name="Subscribe"), 300):
        return True
    return False


def _dzen_publish_access_denied(page, publisher_id: str | None = None) -> bool:
    if _dzen_studio_markers_visible(page):
        return False
    if _dzen_public_channel_view(page):
        return True

    url = _page_url(page)
    if publisher_id:
        pid = publisher_id.strip().lower()
        if f"/profile/editor/id/{pid}" not in url:
            return True
        return False

    if "/profile/editor/" not in url and ("dzen.ru" in url or "zen.yandex" in url):
        return True
    return False


def _rutube_publish_access_denied(page) -> bool:
    url = _page_url(page)
    if "studio.rutube.ru" in url:
        return False
    if _rutube_login_ui(page):
        return True
    if "rutube.ru" in url:
        return True
    return False


def _vkvideo_publish_access_denied(page, club_id: str | None = None) -> bool:
    url = _page_url(page)
    if "cabinet.vkvideo.ru" in url:
        if club_id:
            normalized = club_id.strip().lstrip("@")
            if normalized and f"club{normalized}" not in url.replace("@", ""):
                return True
        return False
    if _vkvideo_login_ui(page):
        return True
    if "vkvideo.ru" in url or "vk.com" in url:
        return True
    return False


def login_screen_visible(page, platform: str, **context) -> bool:
    """True если страница или модал требуют повторной авторизации."""
    if _url_indicates_login(_page_url(page), platform):
        return True
    if platform == "rutube":
        if _rutube_login_ui(page):
            return True
        return _rutube_publish_access_denied(page)
    if platform == "dzen":
        if _dzen_login_ui(page):
            return True
        return _dzen_publish_access_denied(page, context.get("publisher_id"))
    if platform == "vkvideo":
        if _vkvideo_login_ui(page):
            return True
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


def _click_safe_free_field(page, inset: int = 24) -> bool:
    """Клик в свободную область viewport (не в интерактивный контрол)."""
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


def _dismiss_unknown_overlay(page, batch_id, category, label: str) -> None:
    """Закрывает неизвестный оверлей: клик снаружи → Escape → ×."""
    _user_lvl = "info" if batch_id else "silent"
    if _click_safe_free_field(page):
        write_log_entry(
            batch_id, category,
            f"{label}: Закрываю оверлей — клик в свободную область.",
            level=_user_lvl,
        )
        page.wait_for_timeout(300)
        return
    _try_escape(page)
    page.wait_for_timeout(300)
    if _try_generic_close(page):
        write_log_entry(
            batch_id, category,
            f"{label}: Закрываю оверлей — кнопка закрытия.",
            level=_user_lvl,
        )
        page.wait_for_timeout(300)


def dismiss_overlays(
    page,
    whitelist: Sequence[WhitelistEntry],
    batch_id=None,
    category=None,
    *,
    label: str = "",
) -> None:
    """
    Whitelist: известные элементы (detect + optional handle).
    handle=None — не трогать. Иначе — всё неизвестное закрывается dismiss.
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
    if label:
        _dismiss_unknown_overlay(page, batch_id, category, label)


def safe_click(
    locator,
    page,
    whitelist: Sequence[WhitelistEntry],
    *,
    batch_id=None,
    category=None,
    label: str = "",
    timeout_ms: int = 30_000,
    max_attempts: int = 5,
    click_kwargs: dict[str, Any] | None = None,
    js_fallback: bool = False,
) -> None:
    """dismiss_overlays → click; при блокировке повторяет до max_attempts."""
    opts = dict(click_kwargs or {})
    last_err: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        dismiss_overlays(page, whitelist, batch_id, category, label=label)
        try:
            locator.click(timeout=timeout_ms, **opts)
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
            page.wait_for_timeout(500)
    if last_err is not None:
        raise last_err
