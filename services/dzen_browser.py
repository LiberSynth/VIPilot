"""
Менеджер Playwright-браузера для авторизации на Дзен.

Запускает headless Chromium в фоновом потоке, транслирует скриншоты через SSE.
Пользователь авторизуется — после входа нажимает «Сохранить сессию», куки
сохраняются в поле targets.session_context в БД.

Публичный API (потокобезопасен):
    start(target_id)              — запустить браузер
    stop()                        — остановить браузер
    send_event(ev)                — передать событие мыши/клавиатуры
    request_save(target_id)       — сохранить куки в БД
    get_status()                  — текущий статус
    get_session_saved_at(tid)     — ISO-метка последнего сохранения
    profile_exists(tid)           — True если сессия есть в БД
    frame_generator()             — SSE-генератор кадров (JPEG, base64)
    push_frame(img)               — поместить кадр в буфер трансляции
    push_frame_for_batch(bid, img)— кадр для конкретного батча
    get_frame_for_batch(bid)      — последний кадр батча
    run_pipeline_browser(fn, cookies) — запустить публикацию
"""

import base64
import os
import queue
import threading
import time
from datetime import datetime, timezone
from typing import Optional

from log import log_entry

# ---------------------------------------------------------------------------
# Путь к персистентному профилю Chrome
# ---------------------------------------------------------------------------

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DZEN_PROFILE_DIR = os.path.join(_PROJECT_ROOT, "data", "dzen_profile")


def get_session_saved_at(target_id: str | None = None) -> str | None:
    """Возвращает ISO-метку последнего сохранения из БД, или None."""
    if not target_id:
        return None
    from db import db_get_target_session_context_saved_at
    return db_get_target_session_context_saved_at(target_id)


def profile_exists(target_id: str | None = None) -> bool:
    """True если куки Дзена уже сохранены в БД для данного таргета."""
    if not target_id:
        return False
    from db import db_get_target_session_context
    return db_get_target_session_context(target_id) is not None


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
_pipeline_taking_over: bool = False


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
        log_entry(None, f"[dzen_browser] Ошибка события {ev_type!r}: {e}", level='silent')


# ---------------------------------------------------------------------------
# Фоновый поток браузера
# ---------------------------------------------------------------------------

def _browser_loop(target_id: str):
    global _running, _latest_frame, _frame_counter, _save_result

    from playwright.sync_api import sync_playwright

    _set_status("starting", "Запуск браузера…")
    os.makedirs(DZEN_PROFILE_DIR, exist_ok=True)
    log_entry(None, f"[dzen_browser] Профиль Chrome: {DZEN_PROFILE_DIR}", level='silent')

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
                log_entry(None, f"[dzen_browser] Ошибка навигации: {e}", level='silent')

            _set_status("running")

            while _running:
                # Обработка запроса на сохранение сессии
                if _save_request_event.is_set():
                    _save_request_event.clear()
                    try:
                        from db import db_set_target_session_context
                        cookies = context.cookies(["https://dzen.ru", "https://yandex.ru"])
                        saved_at = datetime.now(timezone.utc).isoformat()
                        state = {"cookies": cookies, "saved_at": saved_at}
                        ok = db_set_target_session_context(_current_target_id, state)
                        if ok:
                            _save_result = {"ok": True, "error": None}
                            log_entry(None, f"[dzen_browser] Сессия сохранена в БД: {len(cookies)} куков, target={_current_target_id}", level='silent')
                        else:
                            _save_result = {"ok": False, "error": "Ошибка записи в БД"}
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
                    log_entry(None, f"[dzen_browser] Ошибка скриншота: {e}", level='silent')

                time.sleep(0.2)

            context.close()

    except Exception as e:
        _set_status("error", str(e))
        log_entry(None, f"[dzen_browser] Критическая ошибка: {e}", level='silent')
        return

    if not _pipeline_taking_over:
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


def push_frame(img: bytes):
    """
    Помещает JPEG-кадр в глобальный буфер трансляции (потокобезопасно).
    Вызывается из потока Playwright — без лишних потоков.
    """
    global _latest_frame, _frame_counter
    with _frame_lock:
        _latest_frame = img
        _frame_counter += 1
    _new_frame_event.set()


# ---------------------------------------------------------------------------
# Хранилище кадров по batch_id (для монитора)
# ---------------------------------------------------------------------------

_batch_frames: dict = {}        # batch_id → JPEG bytes
_batch_frames_lock = threading.Lock()
_MAX_BATCH_FRAMES = 30          # держим не более N батчей


def push_frame_for_batch(batch_id: str, img: bytes):
    """Сохраняет последний JPEG-кадр для конкретного батча."""
    with _batch_frames_lock:
        _batch_frames[batch_id] = img
        if len(_batch_frames) > _MAX_BATCH_FRAMES:
            oldest = next(iter(_batch_frames))
            del _batch_frames[oldest]


def get_frame_for_batch(batch_id: str) -> Optional[bytes]:
    """Возвращает последний JPEG-кадр батча или None."""
    with _batch_frames_lock:
        return _batch_frames.get(batch_id)


def run_pipeline_browser(fn, cookies: list) -> dict:
    """
    Запускает fn(page, context) в новом браузере с куками из cookies.
    Стримит скриншоты в виджет (тот же _latest_frame механизм).
    БЛОКИРУЕТ вызывающий поток — вызывать из фонового потока пайплайна.
    Возвращает {"ok": bool, "result": ..., "error": str|None}.
    """
    global _running, _pipeline_taking_over

    # Сигнализируем ДО остановки login-браузера — frame_generator не пошлёт STOPPED
    _pipeline_taking_over = True
    _set_status("running", "Публикация…")

    # Если login-браузер запущен — останавливаем его автоматически
    with _lock:
        if _running:
            _running = False
    if _thread and _thread.is_alive():
        _thread.join(timeout=5)

    with _lock:
        _running = True

    result: dict = {"ok": False, "error": "Неизвестная ошибка"}

    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as pw:
            browser = pw.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
            )
            ctx = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                locale="ru-RU",
                viewport={"width": _VIEWPORT_W, "height": _VIEWPORT_H},
            )
            if cookies:
                try:
                    ctx.add_cookies(cookies)
                    log_entry(None, f"[dzen_pipeline] Загружено {len(cookies)} куков", level='silent')
                except Exception as e:
                    log_entry(None, f"[dzen_pipeline] Ошибка куков: {e}", level='silent')

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

    _pipeline_taking_over = False
    with _lock:
        _running = False
    _set_status("stopped")

    return result


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
            # Ждём секунду — pipeline-браузер может тут же подхватить стрим
            time.sleep(1.0)
            if get_status()["status"] != "stopped":
                continue  # Новый браузер запустился — продолжаем стримить
            yield "data: STOPPED\n\n"
            break
        else:
            yield ": keepalive\n\n"
