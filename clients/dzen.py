"""
Дзен-клиент: публикует короткое видео (gif-формат Дзен) через внутренний editor API.

Все JSON API-вызовы выполняются через page.evaluate(fetch(...)) — в контексте браузера,
что обеспечивает корректную передачу ВСЕХ cookies (включая SameSite=Strict и кук
yandex.ru, которые context.request не передаёт корректно).

TUS-загрузка (шаг 4) выполняется через context.request — CDN-URL presigned, Дзен-сессия
там не нужна.

Полный цикл (7 шагов):
  1. validate-video   — проверка формата/размера (необязательный)
  2. add-publication  — создание черновика
  3. start-upload-video — инициализация TUS
  4. TUS upload (HEAD + PATCH) через context.request
  5. finish-upload-video
  6. poll publication  — ожидание конвертации
  7. update-publication-content-and-publish — финальная публикация
"""

import base64
import json
import time
from urllib.parse import urlencode, urlparse

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
# page.evaluate() fetch-хелпер — вызывает fetch() прямо в JS-контексте браузера
# ---------------------------------------------------------------------------

def _build_url(base: str, params: dict) -> str:
    """Добавляет query-параметры к URL."""
    return f"{base}?{urlencode(params)}" if params else base


def _page_fetch(page, url: str, *,
                method: str = "GET",
                headers: dict | None = None,
                json_body=None,
                binary_body: bytes | None = None) -> dict:
    """
    Выполняет fetch() в JS-контексте страницы (page.evaluate).
    Возвращает {"status": int, "body": str}.
    Все cookies браузера передаются автоматически — в т.ч. SameSite=Strict.
    """
    h = json.dumps(headers or {})

    if binary_body is not None:
        b64 = base64.b64encode(binary_body).decode()
        js = f"""async () => {{
            const b64 = {json.dumps(b64)};
            const bin = atob(b64);
            const bytes = new Uint8Array(bin.length);
            for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
            const resp = await fetch({json.dumps(url)}, {{
                method: {json.dumps(method)},
                headers: {h},
                body: bytes,
                credentials: 'include'
            }});
            const body = await resp.text();
            return {{status: resp.status, body}};
        }}"""
    elif json_body is not None:
        body_str = json.dumps(json_body) if not isinstance(json_body, str) else json_body
        js = f"""async () => {{
            const resp = await fetch({json.dumps(url)}, {{
                method: {json.dumps(method)},
                headers: {h},
                body: {json.dumps(body_str)},
                credentials: 'include'
            }});
            const body = await resp.text();
            return {{status: resp.status, body}};
        }}"""
    else:
        js = f"""async () => {{
            const resp = await fetch({json.dumps(url)}, {{
                method: {json.dumps(method)},
                headers: {h},
                credentials: 'include'
            }});
            const body = await resp.text();
            return {{status: resp.status, body}};
        }}"""

    return page.evaluate(js)


def _check_page_response(result: dict, step: str) -> dict:
    """Проверяет ответ из _page_fetch и возвращает распарсенный JSON."""
    status = result.get("status", 0)
    raw = result.get("body", "")
    print(f"[dzen] {step} HTTP {status}: {raw[:300]}")

    try:
        body = json.loads(raw)
    except Exception:
        raise DzenApiError(f"{step}: нечитаемый ответ ({status}): {raw[:200]}")

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
) -> bool:
    """
    Публикует видео на Дзен.

    target_config ожидается: {"publisher_id": "..."}

    Браузер запускается с персистентным профилем Chrome из data/dzen_profile/ —
    куки читаются нативно без сериализации.

    Возвращает True при успехе.
    """
    import os as _os
    import threading
    from playwright.sync_api import sync_playwright
    from services.dzen_browser import DZEN_PROFILE_DIR, profile_exists

    cfg = target_config or {}
    publisher_id = cfg.get("publisher_id", "")

    if not publisher_id:
        raise DzenApiError("publisher_id не задан в настройках Дзен")

    if not profile_exists():
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
        _os.makedirs(DZEN_PROFILE_DIR, exist_ok=True)
        context = pw.chromium.launch_persistent_context(
            user_data_dir=DZEN_PROFILE_DIR,
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            locale="ru-RU",
        )

        # ── Шаг 0: CSRF из куки (нативный профиль Chrome — HttpOnly доступны) ─
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

        # Перехватываем CSRF и реальный publisherId из браузерных XHR-запросов
        import re as _re
        _real_publisher_id = [None]

        def on_request(req):
            url = req.url
            # Перехват CSRF
            if not csrf_value[0]:
                token = req.headers.get("x-csrf-token")
                if token:
                    csrf_value[0] = token
                    _csrf_source[0] = "xhr"
                    print(f"[dzen] CSRF перехвачен из XHR: {url[:70]}")
                    csrf_ready.set()
            # Перехват publisherId из реальных браузерных запросов к editor-api
            if not _real_publisher_id[0] and "editor-api" in url:
                m = _re.search(r"[?&]publisherId=([^&]+)", url)
                if m:
                    pid = m.group(1)
                    print(f"[dzen] publisherId из браузера: {pid}")
                    _real_publisher_id[0] = pid

        page.on("request", on_request)

        # Навигация: сначала главная для CSRF, потом профиль редактора для publisherId
        try:
            page.goto(
                "https://dzen.ru",
                wait_until="domcontentloaded",
                timeout=_PLAYWRIGHT_NAV_TIMEOUT,
            )
        except Exception as e:
            context.close()
            raise DzenApiError(f"Навигация не удалась: {e}")

        current_url = page.url
        if "passport.yandex" in current_url or "/auth" in current_url:
            context.close()
            raise DzenCsrfExpired(
                "Сессия Дзен истекла — авторизуйтесь снова в браузере (вкладка «Публикация»)"
            )

        # Ждём CSRF из XHR
        if not csrf_value[0]:
            csrf_ready.wait(timeout=_CSRF_WAIT_TIMEOUT)

        if not csrf_value[0]:
            context.close()
            raise DzenApiError(
                "Не удалось получить CSRF-токен. Возможно сессия истекла — "
                "авторизуйтесь снова в браузере (вкладка «Публикация»)"
            )

        # Навигация в студию канала для перехвата реального publisherId из XHR
        if not _real_publisher_id[0] and publisher_id:
            try:
                page.goto(
                    f"https://dzen.ru/profile/editor/id/{publisher_id}/",
                    wait_until="domcontentloaded",
                    timeout=_PLAYWRIGHT_NAV_TIMEOUT,
                )
                page.wait_for_timeout(3000)
            except Exception:
                pass

        # Логируем все cookies для диагностики
        try:
            _all_cookies = context.cookies(["https://dzen.ru", "https://yandex.ru"])
            cookie_names = [c["name"] for c in _all_cookies]
            print(f"[dzen] Куки в сессии: {cookie_names}")
        except Exception:
            pass

        # Определяем итоговый publisherId
        actual_publisher_id = _real_publisher_id[0] or publisher_id
        if _real_publisher_id[0] and _real_publisher_id[0] != publisher_id:
            print(f"[dzen] publisherId из браузера ({_real_publisher_id[0]}) отличается от конфига ({publisher_id[:12]}…)")
            if log_id:
                db_log_entry(log_id, f"Дзен: используем publisher_id из браузера: {actual_publisher_id[:12]}…")
        else:
            print(f"[dzen] publisherId: используем из конфига ({publisher_id[:12]}…)")

        csrf = csrf_value[0]
        if log_id:
            db_log_entry(log_id, f"Дзен: авторизация получена (csrf: {_csrf_source[0]}), начинаю публикацию…")

        # Базовые заголовки для API-запросов
        _api_headers = {
            "X-Csrf-Token": csrf,
            "Accept":       "application/json",
            "Content-Type": "application/json",
            "Referer":      "https://dzen.ru",
            "Origin":       "https://dzen.ru",
        }
        _common_params = {"publisherId": actual_publisher_id, "clid": "320"}

        # ── Шаг 1: validate-video (необязательный — не блокируем при ошибке) ─
        if log_id:
            db_log_entry(log_id, "Дзен: шаг 1/7 — валидация видео…")

        try:
            r1 = _page_fetch(
                page,
                _build_url(f"{_MEDIA_BASE}/validate-video", _common_params),
                method="POST",
                headers={**_api_headers, "Content-Type": "video/mp4"},
                binary_body=video_data,
            )
            _check_page_response(r1, "validate-video")
        except DzenApiError as e:
            print(f"[dzen] validate-video пропущен: {e}")
            if log_id:
                db_log_entry(log_id, f"Дзен: шаг 1 — валидация пропущена ({e}), продолжаю…")

        # ── Шаг 2: add-publication (создание черновика) ────────────────────
        if log_id:
            db_log_entry(log_id, "Дзен: шаг 2/7 — создание черновика…")

        draft_body = {
            "publicationType": "gif",
            "content": {"gifContent": {}},
        }
        r2 = _page_fetch(
            page,
            _build_url(f"{_EDITOR_BASE}/add-publication", _common_params),
            method="POST",
            headers=_api_headers,
            json_body=draft_body,
        )
        data2 = _check_page_response(r2, "add-publication")
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
        r3 = _page_fetch(
            page,
            _build_url(f"{_MEDIA_BASE}/start-upload-video", _common_params),
            method="POST",
            headers=_api_headers,
            json_body=upload_init_body,
        )
        data3 = _check_page_response(r3, "start-upload-video")
        upload_url = data3.get("uploadUrl") or data3.get("upload_url")
        upload_id  = data3.get("uploadId")  or data3.get("upload_id")
        if not upload_url:
            raise DzenApiError(f"start-upload-video: нет uploadUrl в ответе: {data3}")

        # ── Шаг 4: TUS upload через context.request (CDN presigned URL) ────
        if log_id:
            db_log_entry(log_id, "Дзен: шаг 4/7 — загрузка видео (TUS)…")

        req_ctx = context.request
        _tus_upload(req_ctx, upload_url, video_data, log_id)

        # ── Шаг 5: finish-upload-video ─────────────────────────────────────
        if log_id:
            db_log_entry(log_id, "Дзен: шаг 5/7 — завершение загрузки…")

        finish_body = {"publicationId": pub_id}
        if upload_id:
            finish_body["uploadId"] = upload_id
        r5 = _page_fetch(
            page,
            _build_url(f"{_MEDIA_BASE}/finish-upload-video", _common_params),
            method="POST",
            headers=_api_headers,
            json_body=finish_body,
        )
        _check_page_response(r5, "finish-upload-video")

        # ── Шаг 6: poll until converted ────────────────────────────────────
        if log_id:
            db_log_entry(log_id, "Дзен: шаг 6/7 — ожидание конвертации…")

        gif = _poll_conversion(page, publisher_id, pub_id, _api_headers, log_id)

        # ── Шаг 7: publish ─────────────────────────────────────────────────
        if log_id:
            db_log_entry(log_id, "Дзен: шаг 7/7 — публикация…")

        _publish_final(page, publisher_id, pub_id, gif, title, _api_headers)

        context.close()

    if log_id:
        db_log_entry(log_id, "Дзен: видео опубликовано успешно")
    return True


# ---------------------------------------------------------------------------
# Шаги TUS (context.request — CDN presigned) и poll/publish (page.evaluate)
# ---------------------------------------------------------------------------

def _tus_upload(req_ctx, upload_url: str, video_data: bytes, log_id) -> None:
    """TUS upload через Playwright APIRequestContext: HEAD для offset, PATCH-чанки."""
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


def _poll_conversion(page, publisher_id: str, pub_id: str, headers: dict, log_id) -> dict:
    """Ждёт isConverted=True. Возвращает gifContent."""
    deadline = time.time() + _POLL_TIMEOUT
    while time.time() < deadline:
        url = _build_url(
            f"{_EDITOR_BASE}/publisher/{publisher_id}/publication/{pub_id}",
            {"clid": "320"},
        )
        r = _page_fetch(page, url, method="GET", headers=headers)
        data = _check_page_response(r, "poll-publication")

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


def _publish_final(page, publisher_id: str, pub_id: str, gif: dict, title: str, headers: dict) -> None:
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
    url = _build_url(
        f"{_EDITOR_BASE}/update-publication-content-and-publish",
        {"publisherId": publisher_id, "clid": "320"},
    )
    r = _page_fetch(page, url, method="POST", headers=headers, json_body=body)
    data = _check_page_response(r, "publish")
    if data.get("result") != "OK":
        raise DzenApiError(f"publish: неожиданный результат: {data}")


# ---------------------------------------------------------------------------
# Публичный API: is_configured
# ---------------------------------------------------------------------------

def is_configured(target_config: dict | None = None, **_) -> bool:
    """Возвращает True если Дзен полностью настроен для публикации."""
    from services.dzen_browser import profile_exists
    cfg = target_config or {}
    has_pub = bool(cfg.get("publisher_id"))
    return has_pub and profile_exists()
