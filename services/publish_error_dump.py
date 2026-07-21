"""
Сохранение JPEG-скриншота при ошибке Playwright-шага публикации.
"""

from __future__ import annotations

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


def _capture_jpeg(page, platform_browser, batch_id) -> tuple[bytes | None, list[str]]:
    reasons: list[str] = []
    if page is not None:
        try:
            return page.screenshot(type="jpeg", quality=85), reasons
        except Exception as exc:
            reasons.append(f"page.screenshot failed: {_trim_msg(exc)}")
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
            f"Не удалось сохранить скрин ошибки: {exc}",
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
