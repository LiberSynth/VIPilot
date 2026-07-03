"""Тесты CDP preview capture, inline fallback и error dump."""

from unittest.mock import MagicMock, patch

import pytest

from services.publish_error_dump import save_publish_error_dump
from services.publish_frame_hub import PublishFrameHub
from services.publish_preview_capture import (
    PublishPreviewCapture,
    needs_inline_preview,
    start_publish_preview_capture,
    stop_publish_preview_capture,
)


class TestPublishFrameHub:
    def test_resume_broadcast_clears_stopped(self):
        hub = PublishFrameHub()
        hub.push("batch-r", b"jpeg")
        hub.clear("batch-r")
        assert hub.is_stopped("batch-r")

        hub.resume_broadcast("batch-r")
        assert not hub.is_stopped("batch-r")

    def test_end_broadcast_clears_frame_without_stop(self):
        hub = PublishFrameHub()
        hub.push("batch-1", b"jpeg")
        assert hub.get_frame("batch-1") == b"jpeg"
        assert not hub.is_stopped("batch-1")

        hub.end_broadcast("batch-1")

        assert hub.get_frame("batch-1") is None
        assert not hub.is_stopped("batch-1")

    def test_clear_marks_stopped(self):
        hub = PublishFrameHub()
        hub.push("batch-2", b"jpeg")
        hub.clear("batch-2")
        assert hub.get_frame("batch-2") is None
        assert hub.is_stopped("batch-2")


class TestPublishPreviewCapture:
    def test_not_inline_before_capture(self):
        assert not needs_inline_preview("unknown-batch")

    def test_inline_mode_when_cdp_unavailable(self):
        batch_id = "test-batch-inline"
        platform_browser = MagicMock()

        def _mark_inline(self):
            self._set_mode("inline")

        with patch.object(PublishPreviewCapture, "_start", _mark_inline):
            start_publish_preview_capture(
                batch_id, "http://127.0.0.1:1", platform_browser,
            )

        try:
            assert needs_inline_preview(batch_id)
        finally:
            stop_publish_preview_capture(batch_id)
            assert not needs_inline_preview(batch_id)

    def test_stop_clears_mode(self):
        batch_id = "test-batch-stop"
        platform_browser = MagicMock()

        with patch(
            "services.publish_preview_capture.PublishPreviewCapture._start",
            lambda self: None,
        ):
            start_publish_preview_capture(
                batch_id, "http://127.0.0.1:1", platform_browser,
            )

        stop_publish_preview_capture(batch_id)
        assert not needs_inline_preview(batch_id)


class TestSavePublishErrorDump:
    def test_prefers_page_screenshot(self, tmp_path):
        page = MagicMock()
        page.screenshot.return_value = b"from-page"
        platform_browser = MagicMock()
        platform_browser.get_frame_for_batch.return_value = (b"from-hub", 0)

        with patch("services.publish_error_dump._DUMP_DIR", tmp_path):
            path = save_publish_error_dump(
                page,
                batch_id="b1",
                category="publish",
                platform="rutube",
                error="boom",
                platform_browser=platform_browser,
            )

        assert path is not None
        from pathlib import Path
        assert Path(path).read_bytes() == b"from-page"
        page.screenshot.assert_called_once()
        platform_browser.get_frame_for_batch.assert_not_called()

    def test_falls_back_to_hub_frame(self, tmp_path):
        page = MagicMock()
        page.screenshot.side_effect = RuntimeError("page closed")
        platform_browser = MagicMock()
        platform_browser.get_frame_for_batch.return_value = (b"from-hub", 1)

        with patch("services.publish_error_dump._DUMP_DIR", tmp_path):
            path = save_publish_error_dump(
                page,
                batch_id="b2",
                platform_browser=platform_browser,
            )

        assert path is not None
        dumped = list(tmp_path.glob("*.jpeg"))
        assert len(dumped) == 1
        assert dumped[0].read_bytes() == b"from-hub"
