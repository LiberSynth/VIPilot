"""Проверка авторизации и доступа к кабинету при публикации."""

from __future__ import annotations

_SESSION_MSG = (
    "Сессия истекла — авторизуйтесь снова в браузере (вкладка «Публикация»)"
)


def _page_url(page) -> str:
    try:
        return page.url.lower()
    except Exception:
        return ""


def _config_id_str(value) -> str:
    if value is None:
        return ""
    return str(value).strip()


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


__all__ = ("login_screen_visible", "raise_if_login_required")
