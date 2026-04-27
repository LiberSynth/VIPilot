"""
Базовый движок Playwright-браузера для платформ Дзен и Рутьюб.

Все специфичные для платформы параметры передаются в конструктор.
Не используется напрямую — только через тонкие обёртки dzen_browser и rutube_browser.
"""

import base64
import os
import queue
import threading
import time
from datetime import datetime, timezone
from typing import Optional

from log import write_log_entry
from utils.utils import fmt_id_msg


class PlatformBrowser:
    """Playwright-браузер для авторизации и публикации на одной платформе."""

    _VIEWPORT_W = 900
    _VIEWPORT_H = 680
    _USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
    _MAX_BATCH_FRAMES = 30

    def __init__(
        self,
        platform_name: str,
        profile_dir: str,
        start_url: str,
        cookie_domains: list,
        thread_name: str,
    ):
        self._platform      = platform_name
        self._profile_dir   = profile_dir
        self._start_url     = start_url
        self._cookie_domains = cookie_domains
        self._thread_name   = thread_name

        self._lock    = threading.Lock()
        self._running = False
        self._thread: Optional[threading.Thread] = None

        self._status      = "stopped"
        self._status_msg  = ""
        self._status_lock = threading.Lock()

        self._latest_frame: Optional[bytes] = None
        self._frame_counter  = 0
        self._frame_lock     = threading.Lock()
        self._new_frame_event = threading.Event()

        self._event_queue: queue.Queue = queue.Queue(maxsize=400)

        self._save_request_event = threading.Event()
        self._save_done_event    = threading.Event()
        self._save_result: Optional[dict] = None

        self._current_target_id: Optional[str] = None
        self._pipeline_taking_over: bool = False

        self._batch_frames: dict = {}
        self._batch_frames_lock  = threading.Lock()

    # ------------------------------------------------------------------
    # DB helpers
    # ------------------------------------------------------------------

    def get_session_saved_at(self, target_id: Optional[str] = None) -> Optional[str]:
        """Возвращает ISO-метку последнего сохранения из БД, или None."""
        if not target_id:
            return None
        from db import db_get_target_session_context_saved_at
        return db_get_target_session_context_saved_at(target_id)

    def profile_exists(self, target_id: Optional[str] = None) -> bool:
        """True если куки уже сохранены в БД для данного таргета."""
        if not target_id:
            return False
        from db import db_get_target_session_context
        return db_get_target_session_context(target_id) is not None

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------

    def _set_status(self, s: str, msg: str = ""):
        with self._status_lock:
            self._status     = s
            self._status_msg = msg

    def get_status(self) -> dict:
        with self._status_lock:
            return {"status": self._status, "msg": self._status_msg}

    # ------------------------------------------------------------------
    # Event handling
    # ------------------------------------------------------------------

    def _process_event(self, page, ev: dict):
        ev_type = ev.get("type", "")
        tag = f"[{self._platform}_browser]"
        try:
            if ev_type == "click":
                page.mouse.click(float(ev["x"]), float(ev["y"]))
            elif ev_type == "move":
                page.mouse.move(float(ev["x"]), float(ev["y"]))
            elif ev_type == "mousedown":
                page.mouse.down()
            elif ev_type == "mouseup":
                page.mouse.up()
            elif ev_type == "keydown":
                key = ev.get("key", "")
                if key:
                    page.keyboard.press(key)
            elif ev_type == "type":
                text = ev.get("text", "")
                if text:
                    page.keyboard.type(text)
            elif ev_type == "scroll":
                page.mouse.wheel(float(ev.get("dx", 0)), float(ev.get("dy", 0)))
            elif ev_type == "navigate":
                url = ev.get("url", "")
                if url:
                    page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        except Exception as e:
            write_log_entry(None, f"{tag} Ошибка события {ev_type!r}: {e}", level='silent')

    # ------------------------------------------------------------------
    # Browser loop (background thread)
    # ------------------------------------------------------------------

    def _browser_loop(self, target_id: str):
        from playwright.sync_api import sync_playwright

        tag = f"[{self._platform}_browser]"
        self._set_status("starting", "Запуск браузера…")
        os.makedirs(self._profile_dir, exist_ok=True)
        write_log_entry(None, f"{tag} Профиль Chrome: {self._profile_dir}", level='silent')

        try:
            with sync_playwright() as pw:
                context = pw.chromium.launch_persistent_context(
                    user_data_dir=self._profile_dir,
                    headless=True,
                    args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
                    viewport={"width": self._VIEWPORT_W, "height": self._VIEWPORT_H},
                    user_agent=self._USER_AGENT,
                    locale="ru-RU",
                )
                page = context.new_page()

                try:
                    page.goto(self._start_url, wait_until="domcontentloaded", timeout=30_000)
                except Exception as e:
                    write_log_entry(None, f"{tag} Ошибка навигации: {e}", level='silent')

                self._set_status("running")

                while self._running:
                    if self._save_request_event.is_set():
                        self._save_request_event.clear()
                        try:
                            from db import db_set_target_session_context
                            cookies  = context.cookies(self._cookie_domains)
                            saved_at = datetime.now(timezone.utc).isoformat()
                            state    = {"cookies": cookies, "saved_at": saved_at}
                            ok = db_set_target_session_context(self._current_target_id, state)
                            if ok:
                                self._save_result = {"ok": True, "error": None}
                                write_log_entry(
                                    None,
                                    fmt_id_msg(
                                        f"{tag} Сессия сохранена в БД: {len(cookies)} куков, target={{}}",
                                        self._current_target_id,
                                    ),
                                    level='silent',
                                )
                            else:
                                self._save_result = {"ok": False, "error": "Ошибка записи в БД"}
                        except Exception as e:
                            self._save_result = {"ok": False, "error": str(e)}
                        self._save_done_event.set()

                    processed = 0
                    while processed < 20:
                        try:
                            ev = self._event_queue.get_nowait()
                            self._process_event(page, ev)
                            processed += 1
                        except queue.Empty:
                            break

                    try:
                        img = page.screenshot(type="jpeg", quality=65)
                        with self._frame_lock:
                            self._latest_frame   = img
                            self._frame_counter += 1
                        self._new_frame_event.set()
                    except Exception as e:
                        write_log_entry(None, f"{tag} Ошибка скриншота: {e}", level='silent')

                    time.sleep(0.2)

                context.close()

        except Exception as e:
            self._set_status("error", str(e))
            write_log_entry(None, f"{tag} Критическая ошибка: {e}", level='silent')
            return

        if not self._pipeline_taking_over:
            self._set_status("stopped")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self, target_id: str) -> dict:
        with self._lock:
            if self._running:
                return {"ok": True, "already": True}

            self._running           = True
            self._current_target_id = target_id
            self._new_frame_event.clear()
            self._save_request_event.clear()
            self._save_done_event.clear()

            while not self._event_queue.empty():
                try:
                    self._event_queue.get_nowait()
                except queue.Empty:
                    break

            self._thread = threading.Thread(
                target=self._browser_loop,
                args=(target_id,),
                daemon=True,
                name=self._thread_name,
            )
            self._thread.start()

        return {"ok": True, "already": False}

    def stop(self) -> dict:
        with self._lock:
            self._running = False
        return {"ok": True}

    def send_event(self, ev: dict) -> bool:
        try:
            self._event_queue.put_nowait(ev)
            return True
        except queue.Full:
            return False

    def request_save(self, target_id: str) -> dict:
        info = self.get_status()
        if info["status"] != "running":
            return {"ok": False, "error": "Браузер не запущен"}

        self._save_result = None
        self._save_done_event.clear()
        self._save_request_event.set()

        if not self._save_done_event.wait(timeout=10):
            return {"ok": False, "error": "Таймаут сохранения сессии"}

        return self._save_result or {"ok": False, "error": "Неизвестная ошибка"}

    def push_frame(self, img: bytes):
        """Помещает JPEG-кадр в буфер трансляции (потокобезопасно)."""
        with self._frame_lock:
            self._latest_frame   = img
            self._frame_counter += 1
        self._new_frame_event.set()

    def push_frame_for_batch(self, batch_id: str, img: bytes):
        """Сохраняет последний JPEG-кадр для конкретного батча."""
        with self._batch_frames_lock:
            self._batch_frames[batch_id] = (img, time.monotonic())
            if len(self._batch_frames) > self._MAX_BATCH_FRAMES:
                oldest = next(iter(self._batch_frames))
                del self._batch_frames[oldest]

    def get_frame_for_batch(self, batch_id: str) -> Optional[tuple]:
        """Возвращает (JPEG bytes, monotonic timestamp) для батча или None."""
        with self._batch_frames_lock:
            return self._batch_frames.get(batch_id)

    def run_pipeline_browser(self, fn, cookies: list) -> dict:
        """
        Запускает fn(page, context) в новом браузере с куками из cookies.
        БЛОКИРУЕТ вызывающий поток — вызывать из фонового потока пайплайна.
        Возвращает {"ok": bool, "result": ..., "error": str|None}.
        """
        tag = f"[{self._platform}_pipeline]"

        self._pipeline_taking_over = True
        self._set_status("running", "Публикация…")

        with self._lock:
            if self._running:
                self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)

        with self._lock:
            self._running = True

        result: dict = {"ok": False, "error": "Неизвестная ошибка"}

        try:
            from playwright.sync_api import sync_playwright
            with sync_playwright() as pw:
                browser = pw.chromium.launch(
                    headless=True,
                    args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
                )
                ctx = browser.new_context(
                    user_agent=self._USER_AGENT,
                    locale="ru-RU",
                    viewport={"width": self._VIEWPORT_W, "height": self._VIEWPORT_H},
                )
                if cookies:
                    try:
                        ctx.add_cookies(cookies)
                        write_log_entry(None, f"{tag} Загружено {len(cookies)} куков", level='silent')
                    except Exception as e:
                        write_log_entry(None, f"{tag} Ошибка куков: {e}", level='silent')

                page = ctx.new_page()

                try:
                    fn_result = fn(page, ctx)
                    result = {"ok": True, "result": fn_result}
                except Exception as e:
                    import traceback
                    traceback.print_exc()
                    result = {"ok": False, "error": str(e)}
                finally:
                    try:
                        browser.close()
                    except Exception:
                        pass

        except Exception as e:
            result = {"ok": False, "error": f"Playwright: {e}"}

        self._pipeline_taking_over = False
        with self._lock:
            self._running = False
        self._set_status("stopped")

        return result

    def frame_generator(self):
        """
        SSE-генератор: выдаёт кадры как base64-encoded JPEG.
        Формат: 'data: <base64>\\n\\n'
        Завершается когда браузер остановлен и все кадры выданы.
        """
        last_counter = -1

        while True:
            status_now = self.get_status()["status"]

            self._new_frame_event.wait(timeout=1.0)
            self._new_frame_event.clear()

            with self._frame_lock:
                counter = self._frame_counter
                frame   = self._latest_frame

            if frame is not None and counter != last_counter:
                last_counter = counter
                b64 = base64.b64encode(frame).decode()
                yield f"data: {b64}\n\n"
            elif status_now == "stopped":
                time.sleep(1.0)
                if self.get_status()["status"] != "stopped":
                    continue
                yield "data: STOPPED\n\n"
                break
            else:
                yield ": keepalive\n\n"
