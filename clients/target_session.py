"""
Загрузка и сохранение браузерной сессии таргета (targets.session_context).

Единая точка для auth-виджета и pipeline: куки в/из БД через Playwright context.
"""

from __future__ import annotations

from datetime import datetime, timezone

from db import db_get_target_session_context, db_set_target_session_context
from log import write_log_entry
from utils.utils import fmt_id_msg

SESSION_MISSING_MSG = (
    "Браузерная сессия не сохранена — "
    "авторизуйтесь в браузере (вкладка «Публикация»)"
)

_START_URL_TIMEOUT_MS = 30_000


def _log_prefix(target_name: str | None) -> str:
    return f"{target_name}: " if target_name else ""


def has_saved_cookies(target_id: str) -> bool:
    saved = db_get_target_session_context(target_id)
    return bool(saved and saved.get("cookies"))


def bootstrap_pipeline_page(
    page,
    target_id: str,
    start_url: str,
    *,
    batch_id=None,
    category: str | None = None,
    platform: str | None = None,
    target_name: str | None = None,
) -> int:
    """Load session from DB and open platform start_url (same path as auth widget)."""
    count = load_into_context(
        page.context,
        target_id,
        batch_id=batch_id,
        category=category or "publish",
        platform=platform,
        target_name=target_name,
    )
    if count == 0:
        return 0
    prefix = _log_prefix(target_name)
    try:
        page.goto(start_url, wait_until="domcontentloaded", timeout=_START_URL_TIMEOUT_MS)
        write_log_entry(
            batch_id,
            category or "publish",
            f"{prefix}start_url: {start_url}",
            level="silent",
        )
    except Exception as exc:
        write_log_entry(
            batch_id,
            category or "publish",
            f"{prefix}Ошибка навигации на start_url: {exc}",
            level="warn",
        )
    return count


def load_into_context(
    ctx,
    target_id: str,
    *,
    batch_id=None,
    category: str = "browser",
    platform: str | None = None,
    target_name: str | None = None,
) -> int:
    """Читает session_context из БД и добавляет куки в context. Возвращает число куков."""
    prefix = _log_prefix(target_name)
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
    target_name: str | None = None,
    info_note: str = "",
) -> dict:
    """Сохраняет все куки context в targets.session_context. Возвращает {ok, error}."""
    prefix = _log_prefix(target_name)
    try:
        cookies = ctx.cookies()
        saved_at = datetime.now(timezone.utc).isoformat()
        state = {"cookies": cookies, "saved_at": saved_at}
        ok = db_set_target_session_context(target_id, state)
        if ok:
            write_log_entry(
                batch_id,
                category,
                f"{prefix}Сессия сохранена{info_note}.",
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
            domains = sorted(
                {str(c.get("domain", "")).strip() for c in cookies if c.get("domain")}
            )
            if domains:
                write_log_entry(
                    batch_id,
                    category,
                    prefix + f"Домены куков: {', '.join(domains)}",
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


def refresh_session_after_auth(
    page,
    ctx,
    target_id: str,
    platform: str,
    *,
    batch_id=None,
    category: str = "publish",
    target_name: str | None = None,
    **auth_context,
) -> dict:
    """URL-проверка кабинета, затем snapshot куков в БД (warm-up refresh save)."""
    from services.publish_auth_check import raise_if_login_required

    raise_if_login_required(page, platform, **auth_context)
    return save_from_context(
        ctx,
        target_id,
        batch_id=batch_id,
        category=category,
        platform=platform,
        target_name=target_name,
        info_note=" (после входа)",
    )
