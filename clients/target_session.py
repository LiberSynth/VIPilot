"""
Загрузка и сохранение браузерной сессии таргета (targets.session_context).

Единая точка для auth-виджета и pipeline: куки в/из БД через Playwright context.
"""

from __future__ import annotations

from datetime import datetime, timezone

from db import db_get_target_session_context, db_set_target_session_context
from log import write_log_entry
from utils.utils import fmt_id_msg


def load_into_context(
    ctx,
    target_id: str,
    *,
    batch_id=None,
    category: str = "browser",
    platform: str | None = None,
) -> int:
    """Читает session_context из БД и добавляет куки в context. Возвращает число куков."""
    prefix = f"platform={platform}, " if platform else ""
    try:
        saved = db_get_target_session_context(target_id)
        if not saved or not saved.get("cookies"):
            return 0
        cookies = saved["cookies"]
        ctx.add_cookies(cookies)
        write_log_entry(
            batch_id,
            category,
            f"{prefix}Загружено {len(cookies)} куков из БД",
            level="silent",
        )
        return len(cookies)
    except Exception as exc:
        write_log_entry(
            batch_id,
            category,
            f"{prefix}Не удалось загрузить куки из БД: {exc}",
            level="silent",
        )
        return 0


def save_from_context(
    ctx,
    target_id: str,
    *,
    batch_id=None,
    category: str = "browser",
    platform: str | None = None,
) -> dict:
    """Сохраняет все куки context в targets.session_context. Возвращает {ok, error}."""
    prefix = f"platform={platform}, " if platform else ""
    try:
        cookies = ctx.cookies()
        saved_at = datetime.now(timezone.utc).isoformat()
        state = {"cookies": cookies, "saved_at": saved_at}
        ok = db_set_target_session_context(target_id, state)
        if ok:
            write_log_entry(
                batch_id,
                category,
                f"{prefix}Сессия сохранена.",
                level="info",
            )
            write_log_entry(
                batch_id,
                category,
                prefix
                + fmt_id_msg(
                    "Сессия сохранена в БД: {} куков, target={}",
                    len(cookies),
                    target_id,
                ),
                level="silent",
            )
            return {"ok": True, "error": None}
        write_log_entry(
            batch_id,
            category,
            f"{prefix}Ошибка сохранения сессии: запись в БД не удалась.",
            level="info",
        )
        return {"ok": False, "error": "Ошибка записи в БД"}
    except Exception as exc:
        write_log_entry(
            batch_id,
            category,
            f"{prefix}Ошибка сохранения сессии: {exc}",
            level="info",
        )
        return {"ok": False, "error": str(exc)}
