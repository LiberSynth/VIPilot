"""
Сохранение JPEG-скриншота при ошибке Playwright-шага публикации.
"""

from __future__ import annotations

import base64
import binascii
from datetime import datetime
from pathlib import Path

from log import write_log_entry

_DUMP_DIR = Path(__file__).resolve().parents[1] / "dumps"


def _dump_filename() -> str:
    now = datetime.now()
    ms = now.microsecond // 1000
    return now.strftime("error %Y-%m-%d %H-%M-%S.") + f"{ms:03d}.jpeg"


def _trim_msg(value: str | Exception, limit: int = 200) -> str:
    text = str(value).replace("\r", " ").replace("\n", " ").strip()
    return text[:limit] if text else "unknown"


def _capture_via_cdp(page) -> tuple[bytes | None, str | None]:
    cdp_session = None
    try:
        cdp_session = page.context.new_cdp_session(page)
        payload = cdp_session.send(
            "Page.captureScreenshot",
            {"format": "jpeg", "quality": 85},
        )
        data = payload.get("data")
        if not data:
            return None, "Page.captureScreenshot returned empty payload"
        return base64.b64decode(data), None
    except (binascii.Error, ValueError) as exc:
        return None, f"Page.captureScreenshot decode failed: {_trim_msg(exc)}"
    except Exception as exc:
        return None, f"Page.captureScreenshot failed: {_trim_msg(exc)}"
    finally:
        if cdp_session is not None:
            try:
                cdp_session.detach()
            except Exception:
                pass


def _capture_jpeg(page, platform_browser, batch_id) -> tuple[bytes | None, list[str]]:
    reasons: list[str] = []
    if page is not None:
        try:
            return page.screenshot(type="jpeg", quality=85, timeout=5_000), reasons
        except Exception as exc:
            reasons.append(f"page.screenshot failed: {_trim_msg(exc)}")
        cdp_img, cdp_err = _capture_via_cdp(page)
        if cdp_img is not None:
            return cdp_img, reasons
        if cdp_err:
            reasons.append(cdp_err)
    else:
        reasons.append("page is None")

    if platform_browser is not None and batch_id:
        try:
            entry = platform_browser.get_frame_for_batch(batch_id)
        except Exception as exc:
            reasons.append(f"get_frame_for_batch failed: {_trim_msg(exc)}")
            entry = None
        if entry:
            return entry[0], reasons
        reasons.append("batch frame is missing in buffer")
    else:
        if platform_browser is None:
            reasons.append("platform_browser is None")
        if not batch_id:
            reasons.append("batch_id is missing")

    if batch_id:
        try:
            from services.publish_frame_hub import get_hub

            hub_img = get_hub().get_frame(batch_id)
            if hub_img:
                return hub_img, reasons
            reasons.append("publish frame hub is empty")
        except Exception as exc:
            reasons.append(f"publish frame hub read failed: {_trim_msg(exc)}")

    return None, reasons


def save_publish_error_dump(
    page=None,
    *,
    batch_id=None,
    category=None,
    platform: str | None = None,
    target_name: str | None = None,
    error: str | None = None,
    platform_browser=None,
) -> str | None:
    """Пишет JPEG в dumps/; возвращает путь или None если кадра нет."""
    img, reasons = _capture_jpeg(page, platform_browser, batch_id)
    if not img:
        label = target_name or platform
        msg = "Скрин ошибки не сохранен: " + "; ".join(reasons)
        if error:
            msg += f", error={_trim_msg(error)}"
        if label:
            msg = f"{label}: {msg}"
        write_log_entry(batch_id, category or "publish", msg, level="warn")
        return None

    _DUMP_DIR.mkdir(parents=True, exist_ok=True)
    path = _DUMP_DIR / _dump_filename()
    while path.exists():
        path = _DUMP_DIR / _dump_filename()

    try:
        path.write_bytes(img)
    except Exception as exc:
        write_log_entry(
            batch_id, category or "publish",
            f"Не удалось сохранить скрин ошибки: {_trim_msg(exc)}",
            level="warn",
        )
        return None

    label = target_name or platform
    msg = f"Скрин ошибки: {path}"
    if error:
        msg += f", error={_trim_msg(error)}"
    if label:
        msg = f"{label}: {msg}"
    write_log_entry(batch_id, category or "publish", msg, level="warn")
    return str(path)
