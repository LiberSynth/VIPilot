"""
Обнаружение экрана входа при Playwright-публикации (протухшая сессия).
"""

from __future__ import annotations

_SESSION_MSG = (
    "Сессия истекла — авторизуйтесь снова в браузере (вкладка «Публикация»)"
)


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
            or "id.vk.com" in url
            or "login.vk" in url
            or "/auth" in url
            or "passport" in url
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


def login_screen_visible(page, platform: str) -> bool:
    """True если страница или модал требуют повторной авторизации."""
    if _url_indicates_login(_page_url(page), platform):
        return True
    if platform == "rutube":
        return _rutube_login_ui(page)
    if platform == "dzen":
        return _dzen_login_ui(page)
    if platform == "vkvideo":
        return _vkvideo_login_ui(page)
    return False


def raise_if_login_required(page, platform: str) -> None:
    """Бросает *CsrfExpired платформы, если виден экран входа."""
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
