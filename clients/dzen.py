"""
Дзен-клиент: публикует короткое видео (gif-формат Дзен) через внутренний editor API.

Авторизация через Playwright storage state (cookies + localStorage), сохранённый
в поле targets.browser_session.  Все API-вызовы выполняются через
context.request (Playwright APIRequestContext), который автоматически
прикрепляет все куки из storage_state (включая HttpOnly, SameSite=Strict).
CSRF-токен перехватывается из заголовков сетевых запросов страницы.

Загрузка видео — протокол TUS через CDN; бинарные PATCH-запросы также
выполняются через context.request.fetch(), не покидая браузерный контекст.

Полный цикл (7 шагов):
  1. validate-video   — проверка формата/размера
  2. add-publication  — создание черновика
  3. start-upload-video — инициализация TUS
  4. TUS upload (HEAD + PATCH) через context.request
  5. finish-upload-video
  6. poll publication  — ожидание конвертации
  7. update-publication-content-and-publish — финальная публикация
"""

import json
import time
from urllib.parse import urlparse

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
# Вспомогательные функции для ответов Playwright APIResponse
# ---------------------------------------------------------------------------

def _check_api_response(resp, step: str) -> dict:
    """Проверяет APIResponse от context.request и возвращает распарсенный JSON."""
    try:
        body = resp.json()
    except Exception:
        try:
            text = resp.text()
        except Exception:
            text = f"(status {resp.status})"
        raise DzenApiError(
            f"{step}: нечитаемый ответ ({resp.status}): {text[:200]}"
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

    Все шаги выполняются внутри одного браузерного контекста Playwright
    (context.request), который автоматически управляет сессионными куками.

    Возвращает True при успехе.
    """
    import threading
    from playwright.sync_api import sync_playwright

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

    csrf_value: list[str | None] = [None]
    csrf_ready = threading.Event()

    if log_id:
        db_log_entry(log_id, "Дзен: запускаю headless-браузер для авторизации…")

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
        # ── Шаг 0: CSRF из куки (Playwright читает HttpOnly напрямую) ─────────
        _CSRF_COOKIE_NAMES = ("csrftoken2", "_csrf", "csrftoken", "XSRF-TOKEN")
        _csrf_source = ["unknown"]
        try:
            _cookies = context.cookies(["https://dzen.ru", "https://yandex.ru"])
            for _c in _cookies:
                if _c["name"] in _CSRF_COOKIE_NAMES and _c.get("value"):
                    csrf_value[0] = _c["value"]
                    csrf_ready.set()
                    _csrf_source[0] = f"cookie:{_c['name']}"
                    print(f"[dzen] CSRF найден в куке: {_c['name']}")
                    break
        except Exception:
            pass

        page = context.new_page()

        # Дополнительно перехватываем CSRF из заголовков XHR-запросов страницы
        def on_request(req):
            if csrf_value[0]:
                return
            token = req.headers.get("x-csrf-token")
            if token:
                csrf_value[0] = token
                _csrf_source[0] = "xhr"
                print(f"[dzen] CSRF перехвачен из XHR: {req.url[:60]}")
                csrf_ready.set()

        page.on("request", on_request)

        # Навигация нужна для проверки актуальности сессии
        try:
            page.goto(
                "https://dzen.ru",
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

        # Если куки не дали CSRF — ждём XHR-перехвата из страницы
        if not csrf_value[0]:
            csrf_ready.wait(timeout=_CSRF_WAIT_TIMEOUT)

        if not csrf_value[0]:
            browser.close()
            raise DzenApiError(
                "Не удалось получить CSRF-токен. Возможно сессия истекла — "
                "авторизуйтесь снова в браузере (вкладка «Публикация»)"
            )

        csrf = csrf_value[0]
        if log_id:
            db_log_entry(log_id, f"Дзен: авторизация получена (csrf: {_csrf_source[0]}), начинаю публикацию…")

        # Базовые заголовки для всех API-запросов через browser context
        _api_headers = {
            "X-Csrf-Token": csrf,
            "Accept":       "application/json",
            "Content-Type": "application/json",
            "Referer":      "https://dzen.ru",
            "Origin":       "https://dzen.ru",
        }

        req_ctx = context.request

        # ── Шаг 1: validate-video (необязательный — не блокируем при ошибке) ─
        if log_id:
            db_log_entry(log_id, "Дзен: шаг 1/7 — валидация видео…")

        try:
            resp1 = req_ctx.fetch(
                f"{_MEDIA_BASE}/validate-video",
                method="POST",
                params={"publisherId": publisher_id, "clid": "320"},
                headers={**_api_headers, "Content-Type": "video/mp4"},
                data=video_data,
            )
            print(f"[dzen] validate-video HTTP {resp1.status}: {resp1.text()[:400]}")
            _check_api_response(resp1, "validate-video")
        except DzenApiError as e:
            print(f"[dzen] validate-video пропущен (ошибка): {e}")
            if log_id:
                db_log_entry(log_id, f"Дзен: шаг 1 — валидация пропущена ({e}), продолжаю…")

        # ── Шаг 2: add-publication (создание черновика) ────────────────────
        if log_id:
            db_log_entry(log_id, "Дзен: шаг 2/7 — создание черновика…")

        draft_body = {
            "publicationType": "gif",
            "content": {"gifContent": {}},
        }
        resp2 = req_ctx.fetch(
            f"{_EDITOR_BASE}/add-publication",
            method="POST",
            params={"publisherId": publisher_id, "clid": "320"},
            headers=_api_headers,
            data=json.dumps(draft_body).encode(),
        )
        print(f"[dzen] add-publication HTTP {resp2.status}: {resp2.text()[:400]}")
        data2 = _check_api_response(resp2, "add-publication")
        pub_id = data2.get("id") or (data2.get("publications") or [{}])[0].get("id")
        if not pub_id:
            raise DzenApiError(f"add-publication: не найден id в ответе: {data2}")

        if log_id:
            db_log_entry(log_id, f"Дзен: черновик создан id={pub_id}")

        # ── Шаг 3: start-upload-video (инициализация TUS) ─────────────────
        if log_id:
            db_log_entry(log_id, "Дзен: шаг 3/7 — инициализация загрузки…")

        video_size = len(video_data)
        upload_init_body = {
            "publicationId": pub_id,
            "fileName": "video.mp4",
            "fileSize": video_size,
            "mimeType": "video/mp4",
        }
        resp3 = req_ctx.fetch(
            f"{_MEDIA_BASE}/start-upload-video",
            method="POST",
            params={"publisherId": publisher_id, "clid": "320"},
            headers=_api_headers,
            data=json.dumps(upload_init_body).encode(),
        )
        data3 = _check_api_response(resp3, "start-upload-video")
        upload_url = data3.get("uploadUrl") or data3.get("upload_url")
        upload_id  = data3.get("uploadId")  or data3.get("upload_id")
        if not upload_url:
            raise DzenApiError(f"start-upload-video: нет uploadUrl в ответе: {data3}")

        # ── Шаг 4: TUS upload через context.request ────────────────────────
        if log_id:
            db_log_entry(log_id, "Дзен: шаг 4/7 — загрузка видео (TUS)…")

        _tus_upload(req_ctx, upload_url, video_data, log_id)

        # ── Шаг 5: finish-upload-video ─────────────────────────────────────
        if log_id:
            db_log_entry(log_id, "Дзен: шаг 5/7 — завершение загрузки…")

        finish_body = {"publicationId": pub_id}
        if upload_id:
            finish_body["uploadId"] = upload_id
        resp5 = req_ctx.fetch(
            f"{_MEDIA_BASE}/finish-upload-video",
            method="POST",
            params={"publisherId": publisher_id, "clid": "320"},
            headers=_api_headers,
            data=json.dumps(finish_body).encode(),
        )
        _check_api_response(resp5, "finish-upload-video")

        # ── Шаг 6: poll until converted ────────────────────────────────────
        if log_id:
            db_log_entry(log_id, "Дзен: шаг 6/7 — ожидание конвертации…")

        gif = _poll_conversion(req_ctx, publisher_id, pub_id, _api_headers, log_id)

        # ── Шаг 7: publish ─────────────────────────────────────────────────
        if log_id:
            db_log_entry(log_id, "Дзен: шаг 7/7 — публикация…")

        _publish_final(req_ctx, publisher_id, pub_id, gif, title, _api_headers)

        browser.close()

    if log_id:
        db_log_entry(log_id, "Дзен: видео опубликовано успешно")
    return True


# ---------------------------------------------------------------------------
# Шаги TUS и poll (используют context.request / APIRequestContext)
# ---------------------------------------------------------------------------

def _tus_upload(req_ctx, upload_url: str, video_data: bytes, log_id) -> None:
    """TUS upload через Playwright APIRequestContext: HEAD для offset, PATCH-чанки."""
    # HEAD — текущий offset
    tus_headers = {"Tus-Resumable": "1.0.0"}

    head_resp = req_ctx.fetch(
        upload_url,
        method="HEAD",
        headers=tus_headers,
        fail_on_status_code=False,
    )
    offset = int(head_resp.headers.get("upload-offset") or "0")

    chunk_size = 4 * 1024 * 1024
    total = len(video_data)

    while offset < total:
        chunk = video_data[offset: offset + chunk_size]
        patch_resp = req_ctx.fetch(
            upload_url,
            method="PATCH",
            headers={
                "Tus-Resumable":  "1.0.0",
                "Content-Type":   "application/offset+octet-stream",
                "Upload-Offset":  str(offset),
                "Content-Length": str(len(chunk)),
            },
            data=chunk,
            fail_on_status_code=False,
        )

        if patch_resp.status not in (200, 204):
            raise DzenApiError(f"TUS PATCH: неожиданный статус {patch_resp.status}")

        new_offset = int(patch_resp.headers.get("upload-offset") or str(offset + len(chunk)))
        if log_id:
            db_log_entry(
                log_id,
                f"TUS: загружено {new_offset}/{total} байт ({new_offset * 100 // total}%)",
            )
        offset = new_offset


def _poll_conversion(req_ctx, publisher_id: str, pub_id: str, headers: dict, log_id) -> dict:
    """Ждёт isConverted=True. Возвращает gifContent."""
    deadline = time.time() + _POLL_TIMEOUT
    while time.time() < deadline:
        resp = req_ctx.fetch(
            f"{_EDITOR_BASE}/publisher/{publisher_id}/publication/{pub_id}",
            method="GET",
            params={"clid": "320"},
            headers=headers,
        )
        data = _check_api_response(resp, "poll-publication")

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


def _publish_final(req_ctx, publisher_id: str, pub_id: str, gif: dict, title: str, headers: dict) -> None:
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
    resp = req_ctx.fetch(
        f"{_EDITOR_BASE}/update-publication-content-and-publish",
        method="POST",
        params={"publisherId": publisher_id, "clid": "320"},
        headers=headers,
        data=json.dumps(body).encode(),
    )
    data = _check_api_response(resp, "publish")
    if data.get("result") != "OK":
        raise DzenApiError(f"publish: неожиданный результат: {data}")


# ---------------------------------------------------------------------------
# Публичный API: is_configured
# ---------------------------------------------------------------------------

def is_configured(target_config: dict | None = None, browser_session: dict | None = None) -> bool:
    """Возвращает True если Дзен полностью настроен для публикации."""
    cfg = target_config or {}
    has_pub = bool(cfg.get("publisher_id"))
    has_session = bool(browser_session)
    return has_pub and has_session
