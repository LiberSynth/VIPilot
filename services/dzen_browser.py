"""
Менеджер Playwright-браузера для авторизации на Дзен.

Запускает headless Chromium в фоновом потоке, транслирует скриншоты через SSE.
Пользователь авторизуется — куки сохраняются автоматически в профиль Chrome на диске.
Кнопка «Сохранить сессию» фиксирует временную метку без дополнительных действий.

Публичный API (потокобезопасен):
    start(target_id)         — запустить браузер
    stop()                   — остановить браузер
    send_event(ev)           — передать событие мыши/клавиатуры
    request_save(target_id)  — зафиксировать метку сохранения
    get_status()             — текущий статус
    frame_generator()        — SSE-генератор кадров (JPEG, base64)
"""

import base64
import json
import os
import queue
import threading
import time
from datetime import datetime, timezone
from typing import Optional

# ---------------------------------------------------------------------------
# Путь к персистентному профилю Chrome
# ---------------------------------------------------------------------------

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DZEN_PROFILE_DIR = os.path.join(_PROJECT_ROOT, "data", "dzen_profile")
_METADATA_FILE = os.path.join(DZEN_PROFILE_DIR, "_vipilot_meta.json")


def _read_meta() -> dict:
    try:
        with open(_METADATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _write_meta(data: dict):
    os.makedirs(DZEN_PROFILE_DIR, exist_ok=True)
    try:
        with open(_METADATA_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f)
    except Exception as e:
        print(f"[dzen_browser] Ошибка записи метаданных: {e}")


def get_session_saved_at() -> str | None:
    """Возвращает ISO-метку последнего сохранения или None."""
    return _read_meta().get("saved_at")


def profile_exists() -> bool:
    """True если профиль Chrome уже содержит данные (пользователь логинился)."""
    cookies_path = os.path.join(DZEN_PROFILE_DIR, "Default", "Cookies")
    return os.path.exists(cookies_path)


# ---------------------------------------------------------------------------
# Состояние модуля
# ---------------------------------------------------------------------------

_VIEWPORT_W = 900
_VIEWPORT_H = 680

_lock = threading.Lock()
_running = False
_thread: Optional[threading.Thread] = None

_status = "stopped"
_status_msg = ""
_status_lock = threading.Lock()

_latest_frame: Optional[bytes] = None
_frame_counter = 0
_frame_lock = threading.Lock()
_new_frame_event = threading.Event()

_event_queue: queue.Queue = queue.Queue(maxsize=400)

_save_request_event = threading.Event()
_save_done_event = threading.Event()
_save_result: Optional[dict] = None

_current_target_id: Optional[str] = None


# ---------------------------------------------------------------------------
# Вспомогательные
# ---------------------------------------------------------------------------

def _set_status(s: str, msg: str = ""):
    global _status, _status_msg
    with _status_lock:
        _status = s
        _status_msg = msg


def get_status() -> dict:
    with _status_lock:
        return {"status": _status, "msg": _status_msg}


def _process_event(page, ev: dict):
    ev_type = ev.get("type", "")
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
        print(f"[dzen_browser] Ошибка события {ev_type!r}: {e}")


# ---------------------------------------------------------------------------
# Фоновый поток браузера
# ---------------------------------------------------------------------------

def _browser_loop(target_id: str):
    global _running, _latest_frame, _frame_counter, _save_result

    from playwright.sync_api import sync_playwright

    _set_status("starting")
    os.makedirs(DZEN_PROFILE_DIR, exist_ok=True)
    print(f"[dzen_browser] Профиль Chrome: {DZEN_PROFILE_DIR}")

    try:
        with sync_playwright() as pw:
            context = pw.chromium.launch_persistent_context(
                user_data_dir=DZEN_PROFILE_DIR,
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                ],
                viewport={"width": _VIEWPORT_W, "height": _VIEWPORT_H},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                locale="ru-RU",
            )
            page = context.new_page()

            try:
                page.goto(
                    "https://dzen.ru",
                    wait_until="domcontentloaded",
                    timeout=30_000,
                )
            except Exception as e:
                print(f"[dzen_browser] Ошибка навигации: {e}")

            _set_status("running")

            while _running:
                # Обработка запроса на сохранение сессии
                if _save_request_event.is_set():
                    _save_request_event.clear()
                    try:
                        # Куки уже сохранены на диск Chrome автоматически.
                        # Просто фиксируем метку времени.
                        _write_meta({"saved_at": datetime.now(timezone.utc).isoformat()})
                        _save_result = {"ok": True, "error": None}
                        print(f"[dzen_browser] Сессия зафиксирована: {DZEN_PROFILE_DIR}")
                    except Exception as e:
                        _save_result = {"ok": False, "error": str(e)}
                    _save_done_event.set()

                # Обработка событий мыши/клавиатуры
                processed = 0
                while processed < 20:
                    try:
                        ev = _event_queue.get_nowait()
                        _process_event(page, ev)
                        processed += 1
                    except queue.Empty:
                        break

                # Снимок экрана
                try:
                    img = page.screenshot(type="jpeg", quality=65)
                    with _frame_lock:
                        _latest_frame = img
                        _frame_counter += 1
                    _new_frame_event.set()
                except Exception as e:
                    print(f"[dzen_browser] Ошибка скриншота: {e}")

                time.sleep(0.2)

            context.close()

    except Exception as e:
        _set_status("error", str(e))
        print(f"[dzen_browser] Критическая ошибка: {e}")
        return

    _set_status("stopped")


# ---------------------------------------------------------------------------
# Публичный API
# ---------------------------------------------------------------------------

def start(target_id: str) -> dict:
    global _thread, _running, _current_target_id

    with _lock:
        if _running:
            return {"ok": True, "already": True}

        _running = True
        _current_target_id = target_id
        _new_frame_event.clear()
        _save_request_event.clear()
        _save_done_event.clear()

        while not _event_queue.empty():
            try:
                _event_queue.get_nowait()
            except queue.Empty:
                break

        _thread = threading.Thread(
            target=_browser_loop,
            args=(target_id,),
            daemon=True,
            name="dzen-browser",
        )
        _thread.start()

    return {"ok": True, "already": False}


def stop() -> dict:
    global _running

    with _lock:
        _running = False

    return {"ok": True}


def send_event(ev: dict) -> bool:
    try:
        _event_queue.put_nowait(ev)
        return True
    except queue.Full:
        return False


def request_save(target_id: str) -> dict:
    global _save_result

    info = get_status()
    if info["status"] != "running":
        return {"ok": False, "error": "Браузер не запущен"}

    _save_result = None
    _save_done_event.clear()
    _save_request_event.set()

    if not _save_done_event.wait(timeout=10):
        return {"ok": False, "error": "Таймаут сохранения сессии"}

    return _save_result or {"ok": False, "error": "Неизвестная ошибка"}


def frame_generator():
    """
    SSE-генератор: выдаёт кадры как base64-encoded JPEG.
    Формат: 'data: <base64>\\n\\n'
    Завершается когда браузер остановлен и все кадры выданы.
    """
    last_counter = -1

    while True:
        status_now = get_status()["status"]

        _new_frame_event.wait(timeout=1.0)
        _new_frame_event.clear()

        with _frame_lock:
            counter = _frame_counter
            frame = _latest_frame

        if frame is not None and counter != last_counter:
            last_counter = counter
            b64 = base64.b64encode(frame).decode()
            yield f"data: {b64}\n\n"
        elif status_now == "stopped":
            yield "data: STOPPED\n\n"
            break
        else:
            yield ": keepalive\n\n"
