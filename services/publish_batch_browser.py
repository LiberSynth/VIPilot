"""
Общий Chromium для нескольких Playwright-шагов публикации в одном батче.

Между шагами (dzen → rutube → vkvideo) переиспользуется процесс браузера;
на каждый шаг — новый context и куки из БД.
"""

from __future__ import annotations

from typing import Callable

from log import write_log_entry
from services.browser_base import PlatformBrowser

PW_PUBLISH_SLUGS = frozenset({"dzen", "rutube", "vkvideo"})

def pw_step_count(steps: list) -> int:
    """Число Playwright-шагов в списке шагов публикации."""
    return sum(1 for slug, _method, _target in steps if slug in PW_PUBLISH_SLUGS)

def has_pw_steps_after(steps: list, step_idx: int) -> bool:
    """True если после step_idx ещё есть Playwright-шаги."""
    return any(
        steps[i][0] in PW_PUBLISH_SLUGS
        for i in range(step_idx + 1, len(steps))
    )

class PublishBatchBrowserSession:
    """Один Chromium на цепочку PW-шагов publish-батча."""

    def __init__(self, batch_id: str, category, steps: list):
        self.batch_id = batch_id
        self.category = category
        self._steps = steps
        self._step_idx = -1
        self._pw = None
        self._browser = None
        self._open = False

    @property
    def is_open(self) -> bool:
        return self._open

    def set_step_index(self, step_idx: int) -> None:
        self._step_idx = step_idx

    def keep_browser_after_step(self) -> bool:
        if self._step_idx < 0:
            return False
        return has_pw_steps_after(self._steps, self._step_idx)

    def start(self) -> None:
        if self._open:
            return
        import platform as _platform
        from playwright.sync_api import sync_playwright

        _pipeline_args = ["--no-sandbox", "--disable-gpu"]
        if _platform.system() != "Windows":
            _pipeline_args.append("--disable-dev-shm-usage")
        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.launch(
            headless=True,
            args=_pipeline_args,
        )
        self._open = True
        write_log_entry(
            self.batch_id, self.category,
            "Пайплайн: общий браузер Chromium запущен для Playwright-шагов.",
            level="silent",
        )

    def run_step(
        self,
        platform_browser: PlatformBrowser,
        fn: Callable,
        cookies: list,
        batch_id=None,
        category=None,
    ) -> dict:
        if not self._open:
            self.start()
        result: dict = {"ok": False, "error": "Неизвестная ошибка"}
        ctx = self._browser.new_context(
            user_agent=PlatformBrowser._USER_AGENT,
            locale="ru-RU",
            viewport={
                "width": PlatformBrowser._VIEWPORT_W,
                "height": PlatformBrowser._VIEWPORT_H,
            },
        )
        if cookies:
            try:
                ctx.add_cookies(cookies)
                write_log_entry(
                    batch_id, category,
                    f"Загружено {len(cookies)} куков",
                    level="silent",
                )
            except Exception as e:
                write_log_entry(batch_id, category, f"Ошибка куков: {e}", level="silent")
        page = ctx.new_page()
        try:
            fn_result = fn(page, ctx)
            result = {"ok": True, "result": fn_result}
        except Exception as e:
            result = {"ok": False, "error": str(e)}
        finally:
            try:
                ctx.close()
            except Exception:
                pass
        return result

    def close(self) -> None:
        if not self._open:
            return
        try:
            self._browser.close()
        except Exception:
            pass
        try:
            self._pw.stop()
        except Exception:
            pass
        self._browser = None
        self._pw = None
        self._open = False
        write_log_entry(
            self.batch_id, self.category,
            "Пайплайн: общий браузер Chromium закрыт.",
            level="silent",
        )

def finalize_publish_batch_browser(batch_id: str, category) -> None:
    """Сбрасывает превью и статус после закрытия общего браузера пайплайна."""
    from services.browser_registry import clear_publish_frames_for_batch, get_browser

    for slug in PW_PUBLISH_SLUGS:
        try:
            get_browser(slug).stop(batch_id=batch_id, category=category)
        except Exception:
            pass
    if batch_id:
        clear_publish_frames_for_batch(batch_id)
