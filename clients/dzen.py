"""
Дзен-клиент: публикует короткое видео (gif-формат Дзен) через внутренний editor API.

Авторизация через Playwright storage state (cookies + localStorage), сохранённый
в поле targets.browser_session.  CSRF-токен извлекается автоматически при каждой
публикации — запуском headless Chromium с восстановленной сессией.

Загрузка видео — протокол TUS через CDN vu.okcdn.ru.

Полный цикл (7 шагов):
  1. validate-video   — проверка формата/размера
  2. add-publication  — создание черновика
  3. start-upload-video — инициализация TUS
  4. TUS upload (HEAD + PATCH) на CDN
  5. finish-upload-video
  6. poll publication  — ожидание конвертации
  7. update-publication-content-and-publish — финальная публикация
"""

import http.client
import io
import json
import ssl
import time
from urllib.parse import urlparse

import requests

from log import db_log_entry


_EDITOR_BASE = "https://dzen.ru/editor-api/v2"
_MEDIA_BASE  = "https://dzen.ru/media-api-video"
_POLL_TIMEOUT   = 300
_POLL_INTERVAL  = 5

_PLAYWRIGHT_NAV_TIMEOUT = 30_000
_CSRF_WAIT_TIMEOUT = 15


class DzenSessionMissing(RuntimeError):
    """Браузерная сессия Дзен не сохранена — требуется авторизация."""


class DzenCsrfExpired(RuntimeError):
    """CSRF-токен истёк или недействителен."""


class DzenApiError(RuntimeError):
    """Общая ошибка API Дзен."""


# ---------------------------------------------------------------------------
# Получение авторизации через Playwright
# ---------------------------------------------------------------------------

def _get_auth_via_playwright(browser_session: dict, log_id=None) -> tuple[str, dict]:
    """
    Запускает headless Chromium с сохранённой сессией, открывает dzen.ru/profile/editor
    и перехватывает CSRF-токен из первого же запроса к API редактора.

    Возвращает (csrf_token, cookies_dict).
    Бросает DzenCsrfExpired если сессия протухла (редирект на логин).
    Бросает DzenApiError если CSRF не удалось получить.
    """
    import threading
    from playwright.sync_api import sync_playwright

    csrf_value: list[str | None] = [None]
    csrf_ready = threading.Event()

    if log_id:
        db_log_entry(log_id, "Дзен: запускаю headless-браузер для получения авторизации…")

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
        )
        context = browser.new_context(
            storage_state=browser_session,
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            locale="ru-RU",
        )
        page = context.new_page()

        def on_request(req):
            if csrf_value[0]:
                return
            token = req.headers.get("x-csrf-token")
            if token:
                csrf_value[0] = token
                csrf_ready.set()

        page.on("request", on_request)

        try:
            page.goto(
                "https://dzen.ru/profile/editor",
                wait_until="domcontentloaded",
                timeout=_PLAYWRIGHT_NAV_TIMEOUT,
            )
        except Exception as e:
            browser.close()
            raise DzenApiError(f"Навигация не удалась: {e}")

        current_url = page.url
        if "passport.yandex" in current_url or "/auth" in current_url:
            browser.close()
            raise DzenCsrfExpired(
                "Сессия Дзен истекла — авторизуйтесь снова в браузере (вкладка «Публикация»)"
            )

        # Ждём появления CSRF до _CSRF_WAIT_TIMEOUT секунд
        csrf_ready.wait(timeout=_CSRF_WAIT_TIMEOUT)

        raw_cookies = context.cookies()
        browser.close()

    if not csrf_value[0]:
        raise DzenApiError(
            "Не удалось получить CSRF-токен — возможно сессия истекла. "
            "Авторизуйтесь снова в браузере (вкладка «Публикация»)"
        )

    cookies_dict = {c["name"]: c["value"] for c in raw_cookies}
    if log_id:
        db_log_entry(log_id, "Дзен: авторизация получена успешно")
    return csrf_value[0], cookies_dict


def _build_session(csrf: str, cookies: dict) -> requests.Session:
    """Собирает requests.Session с нужными заголовками и куками."""
    session = requests.Session()
    session.cookies.update(cookies)
    session.headers.update({
        "X-Csrf-Token": csrf,
        "Accept":       "application/json",
        "Content-Type": "application/json",
        "User-Agent":   "Mozilla/5.0",
        "Referer":      "https://dzen.ru/profile/editor",
        "Origin":       "https://dzen.ru",
    })
    return session


# ---------------------------------------------------------------------------
# Проверка ошибок
# ---------------------------------------------------------------------------

def _check_errors(resp: requests.Response, step: str) -> dict:
    try:
        body = resp.json()
    except Exception:
        raise DzenApiError(
            f"{step}: нечитаемый ответ ({resp.status_code}): {resp.text[:200]}"
        )

    errors = body.get("errors", [])
    if errors:
        err_type = errors[0].get("type", "")
        if err_type in ("auth-error", "forbidden"):
            raise DzenCsrfExpired(
                f"{step}: авторизация отклонена — авторизуйтесь снова в браузере"
            )
        raise DzenApiError(f"{step}: {errors}")

    return body


# ---------------------------------------------------------------------------
# Шаги публикации
# ---------------------------------------------------------------------------

def _validate_video(session: requests.Session, video_data: bytes, publisher_id: str) -> None:
    headers = dict(session.headers)
    headers["Content-Type"] = "video/mp4"
    resp = session.post(
        f"{_MEDIA_BASE}/validate-video",
        params={"publisherId": publisher_id, "clid": "320"},
        headers=headers,
        data=video_data,
        timeout=60,
    )
    _check_errors(resp, "validate-video")


def _create_draft(session: requests.Session, publisher_id: str) -> str:
    body = {
        "publicationType": "gif",
        "content": {"gifContent": {}},
    }
    resp = session.post(
        f"{_EDITOR_BASE}/add-publication",
        params={"publisherId": publisher_id, "clid": "320"},
        json=body,
        timeout=30,
    )
    data = _check_errors(resp, "add-publication")
    pub_id = data.get("id") or (data.get("publications") or [{}])[0].get("id")
    if not pub_id:
        raise DzenApiError(f"add-publication: не найден id в ответе: {data}")
    return pub_id


def _start_upload(
    session: requests.Session,
    video_data: bytes,
    publisher_id: str,
    pub_id: str,
) -> tuple[str, str, int]:
    """Возвращает (upload_url, upload_id, video_size)."""
    video_size = len(video_data)
    body = {
        "publicationId": pub_id,
        "fileName": "video.mp4",
        "fileSize": video_size,
        "mimeType": "video/mp4",
    }
    resp = session.post(
        f"{_MEDIA_BASE}/start-upload-video",
        params={"publisherId": publisher_id, "clid": "320"},
        json=body,
        timeout=30,
    )
    data = _check_errors(resp, "start-upload-video")
    upload_url = data.get("uploadUrl") or data.get("upload_url")
    upload_id  = data.get("uploadId")  or data.get("upload_id")
    if not upload_url:
        raise DzenApiError(f"start-upload-video: нет uploadUrl в ответе: {data}")
    return upload_url, upload_id, video_size


def _tus_upload(
    session: requests.Session,
    upload_url: str,
    video_data: bytes,
    log_id,
) -> None:
    """TUS upload: HEAD для offset, затем PATCH-чанки."""
    parsed = urlparse(upload_url)
    host = parsed.hostname
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    path = parsed.path + (f"?{parsed.query}" if parsed.query else "")
    ctx = ssl.create_default_context()

    # Извлекаем значение cookie-заголовка
    cookie_header = "; ".join(f"{k}={v}" for k, v in session.cookies.items())

    def _make_tus_headers(extra: dict) -> dict:
        h = {"Tus-Resumable": "1.0.0"}
        if cookie_header:
            h["Cookie"] = cookie_header
        h.update(extra)
        return h

    # HEAD — текущий offset
    conn = http.client.HTTPSConnection(host, port, context=ctx, timeout=30)
    conn.request("HEAD", path, headers=_make_tus_headers({}))
    head_resp = conn.getresponse()
    head_resp.read()
    offset = int(head_resp.getheader("Upload-Offset", "0"))
    conn.close()

    chunk_size = 4 * 1024 * 1024
    total = len(video_data)

    while offset < total:
        chunk = video_data[offset : offset + chunk_size]
        conn2 = http.client.HTTPSConnection(host, port, context=ctx, timeout=300)
        conn2.request(
            "PATCH",
            path,
            body=chunk,
            headers=_make_tus_headers({
                "Content-Type":   "application/offset+octet-stream",
                "Upload-Offset":  str(offset),
                "Content-Length": str(len(chunk)),
            }),
        )
        patch_resp = conn2.getresponse()
        patch_resp.read()
        conn2.close()

        if patch_resp.status not in (200, 204):
            raise DzenApiError(
                f"TUS PATCH: неожиданный статус {patch_resp.status}"
            )

        new_offset = int(patch_resp.getheader("Upload-Offset", offset + len(chunk)))
        if log_id:
            db_log_entry(
                log_id,
                f"TUS: загружено {new_offset}/{total} байт ({new_offset * 100 // total}%)",
            )
        offset = new_offset


def _finish_upload(
    session: requests.Session,
    publisher_id: str,
    pub_id: str,
    upload_id: str | None,
) -> None:
    body = {"publicationId": pub_id}
    if upload_id:
        body["uploadId"] = upload_id
    resp = session.post(
        f"{_MEDIA_BASE}/finish-upload-video",
        params={"publisherId": publisher_id, "clid": "320"},
        json=body,
        timeout=30,
    )
    _check_errors(resp, "finish-upload-video")


def _poll_conversion(
    session: requests.Session,
    publisher_id: str,
    pub_id: str,
    log_id,
) -> dict:
    """Ждёт isConverted=True. Возвращает gifContent."""
    deadline = time.time() + _POLL_TIMEOUT
    while time.time() < deadline:
        resp = session.get(
            f"{_EDITOR_BASE}/publisher/{publisher_id}/publication/{pub_id}",
            params={"clid": "320"},
            timeout=30,
        )
        data = _check_errors(resp, "poll-publication")

        publications = data.get("publications") or [data]
        pub = publications[0] if publications else {}
        gif = (pub.get("content") or {}).get("gifContent") or {}

        if gif.get("isFailed"):
            raise DzenApiError("Конвертация видео завершилась ошибкой (isFailed=True)")

        if gif.get("isConverted"):
            if log_id:
                db_log_entry(log_id, "Видео сконвертировано, публикую…")
            return gif

        if log_id:
            db_log_entry(log_id, "Ожидаю конвертации…")
        time.sleep(_POLL_INTERVAL)

    raise DzenApiError(f"Конвертация не завершилась за {_POLL_TIMEOUT} секунд")


def _publish_final(
    session: requests.Session,
    publisher_id: str,
    pub_id: str,
    gif: dict,
    title: str,
) -> None:
    body = {
        "id": pub_id,
        "preview": {"title": title, "snippet": ""},
        "snippetFrozen": False,
        "hasNativeAds": False,
        "commentsFlagState": "on",
        "delayedPublicationFlagState": "off",
        "visibleComments": "all",
        "visibilityType": "all",
        "premiumTariffs": [],
        "customCommentsTitle": "",
        "gifContent": {
            "videoId":          gif.get("videoId"),
            "isUploaded":       gif.get("isUploaded"),
            "isConverted":      gif.get("isConverted"),
            "isFailed":         gif.get("isFailed", False),
            "autoPublish":      False,
            "duration":         gif.get("duration"),
            "hasSound":         gif.get("hasSound"),
            "size":             gif.get("size"),
            "uploadOnVideoHosting": False,
            "streams":          gif.get("streams", []),
            "thumbnails":       gif.get("thumbnails", []),
            "isHighRes":        gif.get("isHighRes", False),
            "videoMetaId":      gif.get("videoMetaId"),
            "videoFileId":      gif.get("videoFileId"),
            "migratedFromEfir": False,
            "uploadFromYoutube": False,
            "uploadFromCms":    False,
            "parallelsUploadUrls": [],
            "isInvisible":      False,
            "mentions":         [],
            "episodes":         [],
            "externalVideoDuplicates":   [],
            "externalVideoDuplicatesV2": [],
            "oneVideoInfo":     gif.get("oneVideoInfo"),
        },
        "gifMentions": [],
        "tagsInput": {"tags": [], "detectedTagsShown": False},
        "darkPost": False,
    }
    resp = session.post(
        f"{_EDITOR_BASE}/update-publication-content-and-publish",
        params={"publisherId": publisher_id, "clid": "320"},
        json=body,
        timeout=30,
    )
    data = _check_errors(resp, "publish")
    if data.get("result") != "OK":
        raise DzenApiError(f"publish: неожиданный результат: {data}")


# ---------------------------------------------------------------------------
# Публичный API
# ---------------------------------------------------------------------------

def publish(
    video_data: bytes,
    target_config: dict,
    title: str,
    log_id,
    browser_session: dict | None = None,
) -> bool:
    """
    Публикует видео на Дзен.

    target_config ожидается: {"publisher_id": "..."}
    browser_session: Playwright storage state из targets.browser_session.

    Возвращает True при успехе.
    """
    cfg = target_config or {}
    publisher_id = cfg.get("publisher_id", "")

    if not publisher_id:
        raise DzenApiError("publisher_id не задан в настройках Дзен")

    if not browser_session:
        raise DzenSessionMissing(
            "Браузерная сессия Дзен не сохранена — "
            "авторизуйтесь в браузере (вкладка «Публикация»)"
        )

    if log_id:
        db_log_entry(
            log_id,
            f"Дзен: размер видео {len(video_data) // 1024} КБ, publisher={publisher_id[:12]}…",
        )

    # Шаг 0: получаем CSRF и куки из браузерной сессии
    csrf, cookies = _get_auth_via_playwright(browser_session, log_id)
    session = _build_session(csrf, cookies)

    if log_id:
        db_log_entry(log_id, "Дзен: шаг 1/7 — валидация видео…")
    _validate_video(session, video_data, publisher_id)

    if log_id:
        db_log_entry(log_id, "Дзен: шаг 2/7 — создание черновика…")
    pub_id = _create_draft(session, publisher_id)
    if log_id:
        db_log_entry(log_id, f"Дзен: черновик создан id={pub_id}")

    if log_id:
        db_log_entry(log_id, "Дзен: шаг 3/7 — инициализация загрузки…")
    upload_url, upload_id, _ = _start_upload(session, video_data, publisher_id, pub_id)

    if log_id:
        db_log_entry(log_id, "Дзен: шаг 4/7 — загрузка видео (TUS)…")
    _tus_upload(session, upload_url, video_data, log_id)

    if log_id:
        db_log_entry(log_id, "Дзен: шаг 5/7 — завершение загрузки…")
    _finish_upload(session, publisher_id, pub_id, upload_id)

    if log_id:
        db_log_entry(log_id, "Дзен: шаг 6/7 — ожидание конвертации…")
    gif = _poll_conversion(session, publisher_id, pub_id, log_id)

    if log_id:
        db_log_entry(log_id, "Дзен: шаг 7/7 — публикация…")
    _publish_final(session, publisher_id, pub_id, gif, title)

    if log_id:
        db_log_entry(log_id, "Дзен: видео опубликовано успешно")
    return True


def is_configured(target_config: dict | None = None, browser_session: dict | None = None) -> bool:
    """Возвращает True если Дзен полностью настроен для публикации."""
    cfg = target_config or {}
    has_pub = bool(cfg.get("publisher_id"))
    has_session = bool(browser_session)
    return has_pub and has_session
