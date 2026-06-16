"""
Реестр Playwright-браузеров для всех платформ.

Два независимых экземпляра PlatformBrowser на slug:
  - auth     — интерактивная авторизация (виджет «Публикация»)
  - pipeline — headless publish и кадры для Монитора

get_browser() — pipeline (finalize, clients); не менять контракт finalize.

Использование:
    from services.browser_registry import get_auth_browser
    b = get_auth_browser('dzen')
    b.start(target_id)
"""

from services.browser_base import PlatformBrowser

_CONFIGS = {
    "dzen": dict(
        platform_name="dzen",
        start_url="https://dzen.ru",
        cookie_domains=["https://dzen.ru", "https://yandex.ru"],
        thread_name="dzen-browser",
    ),
    "rutube": dict(
        platform_name="rutube",
        start_url="https://studio.rutube.ru/",
        cookie_domains=["https://rutube.ru"],
        thread_name="rutube-browser",
    ),
    "vkvideo": dict(
        platform_name="vkvideo",
        start_url="https://vkvideo.ru/",
        cookie_domains=["https://vkvideo.ru", "https://vk.com", "https://cabinet.vkvideo.ru"],
        thread_name="vkvideo-browser",
    ),
}

SLUGS: tuple[str, ...] = tuple(_CONFIGS.keys())

_auth_browsers: dict[str, PlatformBrowser] = {
    slug: PlatformBrowser(**cfg) for slug, cfg in _CONFIGS.items()
}

_pipeline_browsers: dict[str, PlatformBrowser] = {
    slug: PlatformBrowser(
        **{**cfg, "thread_name": cfg["thread_name"] + "-pipeline"}
    )
    for slug, cfg in _CONFIGS.items()
}


def get_auth_browser(slug: str) -> PlatformBrowser:
    """Интерактивный браузер виджета авторизации."""
    return _auth_browsers[slug]


def get_browser(slug: str) -> PlatformBrowser:
    """Headless publish-браузер (clients, finalize, кадры пайплайна)."""
    return _pipeline_browsers[slug]


def clear_publish_frames_for_batch(batch_id: str) -> None:
    """Удаляет кадры публикации батча из буферов pipeline-браузеров и hub."""
    for slug in SLUGS:
        _pipeline_browsers[slug].clear_frame_for_batch(batch_id)
    from services.publish_frame_hub import get_hub
    get_hub().clear(batch_id)
