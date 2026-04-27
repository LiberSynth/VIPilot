"""
Реестр Playwright-браузеров для всех платформ.

Хранит один экземпляр PlatformBrowser на slug, инициализированный при импорте.

Использование:
    from services.browser_registry import get_browser
    b = get_browser('dzen')
    b.start(target_id)
"""

import os

from services.browser_base import PlatformBrowser

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

_CONFIGS = {
    "dzen": dict(
        platform_name="dzen",
        profile_dir=os.path.join(_PROJECT_ROOT, "data", "dzen_profile"),
        start_url="https://dzen.ru",
        cookie_domains=["https://dzen.ru", "https://yandex.ru"],
        thread_name="dzen-browser",
    ),
    "rutube": dict(
        platform_name="rutube",
        profile_dir=os.path.join(_PROJECT_ROOT, "data", "rutube_profile"),
        start_url="https://studio.rutube.ru/",
        cookie_domains=["https://rutube.ru"],
        thread_name="rutube-browser",
    ),
    "vkvideo": dict(
        platform_name="vkvideo",
        profile_dir=os.path.join(_PROJECT_ROOT, "data", "vkvideo_profile"),
        start_url="https://vkvideo.ru/",
        cookie_domains=["https://vkvideo.ru", "https://vk.com"],
        thread_name="vkvideo-browser",
    ),
}

_browsers: dict[str, PlatformBrowser] = {
    slug: PlatformBrowser(**cfg) for slug, cfg in _CONFIGS.items()
}

SLUGS: tuple[str, ...] = tuple(_CONFIGS.keys())


def get_browser(slug: str) -> PlatformBrowser:
    """Возвращает экземпляр PlatformBrowser для указанного slug."""
    return _browsers[slug]
