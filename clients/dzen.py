"""
Дзен API-клиент.
Публикует короткое видео (gif-формат Дзен) через внутренний editor API.
Протокол: OAuth Bearer + X-Csrf-Token (без браузерных кук).
Загрузка видео — протокол TUS через CDN vu.okcdn.ru.

Полный цикл (7 шагов):
  1. validate-video   — проверка формата/размера
  2. add-publication  — создание черновика
  3. start-upload-video — инициализация TUS
  4. TUS upload (HEAD + PATCH) на CDN
  5. finish-upload-video
  6. poll publication  — ожидание конвертации
  7. update-publication-content-and-publish — финальная публикация

CSRF: хранится в targets.config['csrf_token'].
Срок жизни неизвестен (Яндекс не публикует).
При истечении API возвращает {"errors":[{"type":"auth-error",...}]}.
В этом случае нужно обновить токен в админке.
"""

import io
import os
import time
import urllib.request
import urllib.error
import json

import requests

from log import db_log_entry


_OAUTH_TOKEN = os.environ.get("DZEN_OAUTH_TOKEN", "")

_EDITOR_BASE = "https://dzen.ru/editor-api/v2"
_MEDIA_BASE  = "https://dzen.ru/media-api-video"
_POLL_TIMEOUT   = 300
_POLL_INTERVAL  = 5


class DzenCsrfExpired(RuntimeError):
    """CSRF-токен истёк — нужно обновить в настройках."""


class DzenApiError(RuntimeError):
    """Общая ошибка API Дзен."""


def _headers(csrf: str) -> dict:
    return {
        "Authorization":  f"OAuth {_OAUTH_TOKEN}",
        "X-Csrf-Token":   csrf,
        "Accept":         "application/json",
        "Content-Type":   "application/json",
        "User-Agent":     "Mozilla/5.0",
        "Referer":        "https://dzen.ru/profile/editor",
        "Origin":         "https://dzen.ru",
    }


def _check_errors(resp: requests.Response, step: str) -> dict:
    try:
        body = resp.json()
    except Exception:
        raise DzenApiError(f"{step}: нечитаемый ответ ({resp.status_code}): {resp.text[:200]}")

    errors = body.get("errors", [])
    if errors:
        err_type = errors[0].get("type", "")
        if err_type in ("auth-error", "forbidden"):
            raise DzenCsrfExpired(f"{step}: CSRF-токен истёк или невалиден")
        raise DzenApiError(f"{step}: {errors}")

    return body


def _validate_video(video_data: bytes, csrf: str, publisher_id: str) -> None:
    resp = requests.post(
        f"{_MEDIA_BASE}/validate-video",
        params={"publisherId": publisher_id, "clid": "320"},
        headers={**_headers(csrf), "Content-Type": "video/mp4"},
        data=video_data,
        timeout=60,
    )
    _check_errors(resp, "validate-video")


def _create_draft(csrf: str, publisher_id: str) -> str:
    body = {
        "publicationType": "gif",
        "content": {"gifContent": {}},
    }
    resp = requests.post(
        f"{_EDITOR_BASE}/add-publication",
        params={"publisherId": publisher_id, "clid": "320"},
        headers=_headers(csrf),
        json=body,
        timeout=30,
    )
    data = _check_errors(resp, "add-publication")
    pub_id = data.get("id") or (data.get("publications") or [{}])[0].get("id")
    if not pub_id:
        raise DzenApiError(f"add-publication: не найден id в ответе: {data}")
    return pub_id


def _start_upload(video_data: bytes, csrf: str, publisher_id: str, pub_id: str) -> tuple[str, str, int]:
    """Возвращает (upload_url, upload_id, video_size)."""
    video_size = len(video_data)
    body = {
        "publicationId": pub_id,
        "fileName": "video.mp4",
        "fileSize": video_size,
        "mimeType": "video/mp4",
    }
    resp = requests.post(
        f"{_MEDIA_BASE}/start-upload-video",
        params={"publisherId": publisher_id, "clid": "320"},
        headers=_headers(csrf),
        json=body,
        timeout=30,
    )
    data = _check_errors(resp, "start-upload-video")
    upload_url = data.get("uploadUrl") or data.get("upload_url")
    upload_id  = data.get("uploadId")  or data.get("upload_id")
    if not upload_url:
        raise DzenApiError(f"start-upload-video: нет uploadUrl в ответе: {data}")
    return upload_url, upload_id, video_size


def _tus_upload(upload_url: str, video_data: bytes, log_id) -> None:
    """TUS upload: HEAD для offset, затем PATCH."""
    parsed = urllib.request.urlopen.__doc__  # just to import urllib
    import http.client, ssl

    def _parse_url(url: str):
        from urllib.parse import urlparse
        p = urlparse(url)
        return p.hostname, p.port or (443 if p.scheme == "https" else 80), p.path + (f"?{p.query}" if p.query else "")

    host, port, path = _parse_url(upload_url)
    ctx = ssl.create_default_context()

    # HEAD — узнаём текущий offset
    conn = http.client.HTTPSConnection(host, port, context=ctx, timeout=30)
    conn.request("HEAD", path, headers={
        "Tus-Resumable": "1.0.0",
        "Authorization": f"OAuth {_OAUTH_TOKEN}",
    })
    head_resp = conn.getresponse()
    head_resp.read()
    offset = int(head_resp.getheader("Upload-Offset", "0"))
    conn.close()

    chunk_size = 4 * 1024 * 1024
    total = len(video_data)

    while offset < total:
        chunk = video_data[offset:offset + chunk_size]
        conn2 = http.client.HTTPSConnection(host, port, context=ctx, timeout=300)
        conn2.request("PATCH", path,
            body=chunk,
            headers={
                "Tus-Resumable":   "1.0.0",
                "Content-Type":    "application/offset+octet-stream",
                "Upload-Offset":   str(offset),
                "Content-Length":  str(len(chunk)),
                "Authorization":   f"OAuth {_OAUTH_TOKEN}",
            },
        )
        patch_resp = conn2.getresponse()
        patch_resp.read()
        conn2.close()

        if patch_resp.status not in (200, 204):
            raise DzenApiError(f"TUS PATCH: неожиданный статус {patch_resp.status}")

        new_offset = int(patch_resp.getheader("Upload-Offset", offset + len(chunk)))
        if log_id:
            db_log_entry(log_id, f"TUS: загружено {new_offset}/{total} байт ({new_offset*100//total}%)")
        offset = new_offset


def _finish_upload(csrf: str, publisher_id: str, pub_id: str, upload_id: str | None) -> None:
    body = {"publicationId": pub_id}
    if upload_id:
        body["uploadId"] = upload_id
    resp = requests.post(
        f"{_MEDIA_BASE}/finish-upload-video",
        params={"publisherId": publisher_id, "clid": "320"},
        headers=_headers(csrf),
        json=body,
        timeout=30,
    )
    _check_errors(resp, "finish-upload-video")


def _poll_conversion(csrf: str, publisher_id: str, pub_id: str, log_id) -> dict:
    """Ждёт isConverted=True. Возвращает gifContent."""
    deadline = time.time() + _POLL_TIMEOUT
    while time.time() < deadline:
        resp = requests.get(
            f"{_EDITOR_BASE}/publisher/{publisher_id}/publication/{pub_id}",
            params={"clid": "320"},
            headers=_headers(csrf),
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


def _publish(csrf: str, publisher_id: str, pub_id: str, gif: dict, title: str) -> None:
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
    resp = requests.post(
        f"{_EDITOR_BASE}/update-publication-content-and-publish",
        params={"publisherId": publisher_id, "clid": "320"},
        headers=_headers(csrf),
        json=body,
        timeout=30,
    )
    data = _check_errors(resp, "publish")
    if data.get("result") != "OK":
        raise DzenApiError(f"publish: неожиданный результат: {data}")


def publish(video_data: bytes, target_config: dict, title: str, log_id) -> bool:
    """
    Публикует видео на Дзен.
    target_config ожидается: {"publisher_id": "...", "csrf_token": "..."}
    Опционально: "oauth_token" для переопределения env-переменной.
    Возвращает True при успехе.
    """
    global _OAUTH_TOKEN
    cfg = target_config or {}

    publisher_id = cfg.get("publisher_id", "")
    csrf         = cfg.get("csrf_token", "")
    oauth        = cfg.get("oauth_token", "") or _OAUTH_TOKEN

    if not publisher_id:
        raise DzenApiError("publisher_id не задан в настройках Дзен")
    if not csrf:
        raise DzenApiError("CSRF-токен не задан в настройках Дзен")
    if not oauth:
        raise DzenApiError("OAuth-токен Дзен не задан (DZEN_OAUTH_TOKEN)")

    _OAUTH_TOKEN = oauth

    if log_id:
        db_log_entry(log_id, f"Дзен: размер видео {len(video_data)//1024} КБ, publisher={publisher_id[:12]}…")

    if log_id:
        db_log_entry(log_id, "Дзен: шаг 1/7 — валидация видео…")
    _validate_video(video_data, csrf, publisher_id)

    if log_id:
        db_log_entry(log_id, "Дзен: шаг 2/7 — создание черновика…")
    pub_id = _create_draft(csrf, publisher_id)
    if log_id:
        db_log_entry(log_id, f"Дзен: черновик создан id={pub_id}")

    if log_id:
        db_log_entry(log_id, "Дзен: шаг 3/7 — инициализация загрузки…")
    upload_url, upload_id, _ = _start_upload(video_data, csrf, publisher_id, pub_id)

    if log_id:
        db_log_entry(log_id, "Дзен: шаг 4/7 — загрузка видео (TUS)…")
    _tus_upload(upload_url, video_data, log_id)

    if log_id:
        db_log_entry(log_id, "Дзен: шаг 5/7 — завершение загрузки…")
    _finish_upload(csrf, publisher_id, pub_id, upload_id)

    if log_id:
        db_log_entry(log_id, "Дзен: шаг 6/7 — ожидание конвертации…")
    gif = _poll_conversion(csrf, publisher_id, pub_id, log_id)

    if log_id:
        db_log_entry(log_id, "Дзен: шаг 7/7 — публикация…")
    _publish(csrf, publisher_id, pub_id, gif, title)

    if log_id:
        db_log_entry(log_id, "Дзен: видео опубликовано успешно")
    return True


def is_configured(target_config: dict | None = None) -> bool:
    cfg = target_config or {}
    has_oauth = bool(cfg.get("oauth_token") or _OAUTH_TOKEN)
    has_csrf  = bool(cfg.get("csrf_token"))
    has_pub   = bool(cfg.get("publisher_id"))
    return has_oauth and has_csrf and has_pub


def csrf_token_age_minutes(target_config: dict | None = None) -> int | None:
    """Возвращает возраст CSRF-токена в минутах, или None если не определить."""
    cfg = target_config or {}
    csrf = cfg.get("csrf_token", "")
    if not csrf or ":" not in csrf:
        return None
    try:
        ts_ms = int(csrf.split(":")[-1])
        age_ms = (time.time() * 1000) - ts_ms
        return int(age_ms / 60000)
    except (ValueError, IndexError):
        return None
