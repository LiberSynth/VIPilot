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


def _capture_jpeg(page, platform_browser, batch_id) -> bytes | None:
    if page is not None:
        try:
            return page.screenshot(type="jpeg", quality=85)
        except Exception:
            pass
    if platform_browser is not None and batch_id:
        entry = platform_browser.get_frame_for_batch(batch_id)
        if entry:
            return entry[0]
    return None


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
    img = _capture_jpeg(page, platform_browser, batch_id)
    if not img:
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
        msg += f", error={error[:200]}"
    if label:
        msg = f"{label}: {msg}"
    write_log_entry(batch_id, category or "publish", msg, level="warn")
    return str(path)
