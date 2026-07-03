"""
CDP-трансляция кадров публикации в Монитор (200 ms), отдельно от потока пайплайна.

Подключается к уже запущенному Chromium через connect_over_cdp; пайплайн не трогаем.
"""

from __future__ import annotations

import socket
import threading
import time

from log import write_log_entry

_CAPTURE_INTERVAL_S = 0.2

_registry_lock = threading.Lock()
_active: dict[str, PublishPreviewCapture] = {}


def allocate_cdp_debug_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def cdp_url_for_port(port: int) -> str:
    return f"http://127.0.0.1:{port}"


def start_publish_preview_capture(batch_id: str, cdp_url: str, platform_browser) -> None:
    if not batch_id or not cdp_url:
        return
    with _registry_lock:
        existing = _active.pop(batch_id, None)
        if existing is not None:
            existing._stop()
        cap = PublishPreviewCapture(batch_id, cdp_url, platform_browser)
        _active[batch_id] = cap
        cap._start()


def stop_publish_preview_capture(batch_id: str | None) -> None:
    if not batch_id:
        return
    with _registry_lock:
        cap = _active.pop(batch_id, None)
    if cap is not None:
        cap._stop()


class PublishPreviewCapture:
    def __init__(self, batch_id: str, cdp_url: str, platform_browser):
        self._batch_id = batch_id
        self._cdp_url = cdp_url
        self._platform_browser = platform_browser
        self._running = False
        self._thread: threading.Thread | None = None

    def _start(self) -> None:
        self._running = True
        self._thread = threading.Thread(
            target=self._run,
            daemon=True,
            name=f"pub-preview-{self._batch_id[:8]}",
        )
        self._thread.start()

    def _stop(self) -> None:
        self._running = False
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=3.0)

    def _pick_page(self, browser):
        for ctx in browser.contexts:
            for page in ctx.pages:
                try:
                    if not page.is_closed():
                        return page
                except Exception:
                    continue
        return None

    def _run(self) -> None:
        from playwright.sync_api import sync_playwright

        try:
            with sync_playwright() as pw:
                browser = pw.chromium.connect_over_cdp(self._cdp_url)
                while self._running:
                    page = self._pick_page(browser)
                    if page is not None:
                        try:
                            img = page.screenshot(type="jpeg", quality=65)
                            self._platform_browser.push_frame_for_batch(
                                self._batch_id, img,
                            )
                        except Exception as exc:
                            write_log_entry(
                                self._batch_id,
                                "publish",
                                f"preview_capture screenshot: {exc}",
                                level="silent",
                            )
                    time.sleep(_CAPTURE_INTERVAL_S)
        except Exception as exc:
            write_log_entry(
                self._batch_id,
                "publish",
                f"preview_capture CDP: {exc}",
                level="silent",
            )
