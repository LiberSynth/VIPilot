"""
Обнаружение экрана входа и потери доступа к кабинету при Playwright-публикации.
"""

from __future__ import annotations

_SESSION_MSG = (
    "Сессия истекла — авторизуйтесь снова в браузере (вкладка «Публикация»)"
)

_DZEN_STUDIO_SELECTORS = (
    "[class*='author-studio-header'], "
    "[data-testid='add-publication-button'], "
    "[class*='author-studio-header__addButton'], "
    "[class*='addButton']"
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
    """Публичная страница канала (гость), не студия автора."""
    if _visible(page, page.get_by_role("button", name="Подписаться"), 300):
        return True
    if _visible(page, page.get_by_role("button", name="Subscribe"), 300):
        return True
    return False


def _dzen_publish_access_denied(page, publisher_id: str | None = None) -> bool:
    """
    Студия автора недоступна: редирект на публичный профиль или чужой publisher_id.
    Не срабатывает, пока студия ещё грузится (editor URL без маркеров — ждём).
    """
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
