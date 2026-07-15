"""Проверка авторизации при публикации (Playwright): URL login/passport и DOM Rutube."""

from __future__ import annotations

_SESSION_MSG = (
    "Сессия истекла — авторизуйтесь снова в браузере (вкладка «Публикация»)"
)


def _page_url(page) -> str:
    try:
        return page.url.lower()
    except Exception:
        return ""


def _text_visible(page, text: str, *, exact: bool = False, timeout_ms: int = 200) -> bool:
    try:
        return page.get_by_text(text, exact=exact).first.is_visible(timeout=timeout_ms)
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
        return (
            "passport.yandex" in url
            or "id.yandex.ru" in url
            or "login.yandex" in url
            or "oauth.yandex" in url
            or "auth.yandex" in url
            or "accounts.yandex" in url
            or "dzen.ru/login" in url
            or "dzen.ru/signin" in url
            or "/auth" in url
        )
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


def _rutube_studio_login_modal_visible(page) -> bool:
    """Модалка «Вход» на studio.rutube.ru без редиректа на passport/login."""
    if "studio.rutube.ru" not in _page_url(page):
        return False
    if not _text_visible(page, "Вход", exact=True):
        return False
    if _text_visible(page, "Телефон или почта"):
        return True
    if _text_visible(page, "Зарегистрироваться"):
        return True
    if _text_visible(page, "SmartCaptcha"):
        return True
    try:
        if page.locator(
            "input[placeholder*='елефон'], input[placeholder*='почт'], "
            "input[placeholder*='Телефон'], input[placeholder*='Почт']"
        ).first.is_visible(timeout=200):
            return True
    except Exception:
        pass
    return False


def login_screen_visible(page, platform: str, **context) -> bool:
    """True если экран входа: редирект на passport/login или модалка Rutube в studio."""
    del context
    if _url_indicates_login(_page_url(page), platform):
        return True
    if platform == "rutube" and _rutube_studio_login_modal_visible(page):
        return True
    return False


def raise_if_login_required(page, platform: str, **context) -> None:
    """Бросает *CsrfExpired платформы при необходимости авторизации."""
    del context
    if not login_screen_visible(page, platform):
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


__all__ = ("login_screen_visible", "raise_if_login_required")
