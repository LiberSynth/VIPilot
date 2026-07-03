"""Границы трансляции PW-шага: старт/финиш, без привязки к исходу публикации."""

from __future__ import annotations

from services.publish_frame_hub import get_hub
from services.publish_preview_capture import (
    start_publish_preview_capture,
    stop_publish_preview_capture,
)


def begin_pw_step_broadcast(
    batch_id: str | None,
    *,
    cdp_url: str | None = None,
    platform_browser=None,
) -> None:
    """Старт PW-шага: возобновить hub и CDP-capture."""
    if not batch_id:
        return
    get_hub().resume_broadcast(batch_id)
    if cdp_url and platform_browser is not None:
        start_publish_preview_capture(batch_id, cdp_url, platform_browser)


def end_pw_step_broadcast(batch_id: str | None) -> None:
    """Финиш PW-шага (любой исход): остановить capture и выключить эфир."""
    if not batch_id:
        return
    stop_publish_preview_capture(batch_id)
    get_hub().end_broadcast(batch_id)
