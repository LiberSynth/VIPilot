"""
Менеджер Playwright-браузера для авторизации на Рутьюбе.

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

import os

from services.browser_base import PlatformBrowser

_PROJECT_ROOT       = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RUTUBE_PROFILE_DIR  = os.path.join(_PROJECT_ROOT, "data", "rutube_profile")

_browser = PlatformBrowser(
    platform_name="rutube",
    profile_dir=RUTUBE_PROFILE_DIR,
    start_url="https://studio.rutube.ru/",
    cookie_domains=["https://rutube.ru"],
    thread_name="rutube-browser",
)


def get_session_saved_at(target_id=None):      return _browser.get_session_saved_at(target_id)
def profile_exists(target_id=None):            return _browser.profile_exists(target_id)
def get_status():                              return _browser.get_status()
def start(target_id):                          return _browser.start(target_id)
def stop():                                    return _browser.stop()
def send_event(ev):                            return _browser.send_event(ev)
def request_save(target_id):                   return _browser.request_save(target_id)
def push_frame(img):                           return _browser.push_frame(img)
def push_frame_for_batch(batch_id, img):       return _browser.push_frame_for_batch(batch_id, img)
def get_frame_for_batch(batch_id):             return _browser.get_frame_for_batch(batch_id)
def run_pipeline_browser(fn, cookies):         return _browser.run_pipeline_browser(fn, cookies)
def frame_generator():                         return _browser.frame_generator()
