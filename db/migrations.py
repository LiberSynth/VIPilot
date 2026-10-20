"""
Версионированные миграции схемы БД.

ПРАВИЛА УДАЛЕНИЯ ЗАДЕПЛОЕННЫХ МИГРАЦИЙ
══════════════════════════════════════════════════════════════════════════
Миграция считается «задеплоенной», когда она подтверждённо применена на
всех окружениях (dev + prod). Убедиться можно запросом:
    SELECT value FROM settings WHERE key = 'db_version';

Чтобы безопасно удалить миграцию из кода:
  1. Проверьте, что db_version в prod >= номера удаляемой миграции.
  2. Удалите функцию и её запись из MIGRATIONS.
  3. НЕ переиспользуйте освободившийся номер версии — только вперёд.
  4. Перенесите изменение схемы в bootstrap() (db/init.py).
  5. Ориентир: удалять не раньше двух недель после деплоя на prod.

Схема для развёртывания с нуля — только bootstrap() в db/init.py.
MIGRATIONS — для одноразовых data/backfill шагов при апгрейде (если понадобятся).
══════════════════════════════════════════════════════════════════════════
"""

from .connection import get_db
from log.log import write_log_entry

# ---------------------------------------------------------------------------
# Реестр миграций — добавляйте только в конец, никогда не переиспользуйте номера
# ---------------------------------------------------------------------------

def _migration_0001_t2v_conversion_model_id(cur):
    cur.execute("""
        INSERT INTO cycle_config (key, value)
        SELECT 't2v_conversion_model_id', ''
        WHERE NOT EXISTS (
            SELECT 1 FROM cycle_config WHERE key = 't2v_conversion_model_id'
        )
    """)


_DEEPSEEK_V4_PRO_BODY = (
    '{"max_tokens": 8192,'
    ' "messages": [{"role": "system", "content": "{}"},'
    ' {"role": "user", "content": "{}"}],'
    ' "reasoning_effort": "low",'
    ' "thinking": {"type": "enabled"}}'
)


def _migration_0002_deepseek_v4_pro(cur):
    cur.execute("""
        SELECT 1 FROM ai_models
        WHERE type = 'text' AND url = 'deepseek-v4-pro'
        LIMIT 1
    """)
    if cur.fetchone():
        return

    cur.execute("""
        SELECT id FROM ai_platforms WHERE name = 'DeepSeek' LIMIT 1
    """)
    platform = cur.fetchone()
    if not platform:
        raise RuntimeError("Площадка DeepSeek не найдена в ai_platforms")

    cur.execute("""
        SELECT COALESCE(MIN("order"), 0) FROM ai_models
        WHERE type = 'text' AND url IN ('deepseek-v4-flash', 'deepseek-flash')
    """)
    new_order = cur.fetchone()[0] + 1
    cur.execute(
        """
        UPDATE ai_models
        SET "order" = "order" + 1
        WHERE type = 'text' AND "order" >= %s
        """,
        (new_order,),
    )
    cur.execute(
        """
        INSERT INTO ai_models
            (name, platform_id, type, active, url, body, "order", grade)
        SELECT %s, %s, 'text', TRUE, 'deepseek-v4-pro', %s::jsonb, %s, 'good'
        WHERE NOT EXISTS (
            SELECT 1 FROM ai_models WHERE type = 'text' AND url = 'deepseek-v4-pro'
        )
        """,
        ("deepseek-v4-pro", platform[0], _DEEPSEEK_V4_PRO_BODY, new_order),
    )


MIGRATIONS = [
    (1, _migration_0001_t2v_conversion_model_id),
    (2026102001, _migration_0002_deepseek_v4_pro),
]

# ---------------------------------------------------------------------------
# Запуск миграций
# ---------------------------------------------------------------------------

def run_migrations():
    """Применяет все незапущенные миграции. Каждая в отдельной транзакции."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM settings WHERE key = 'db_version'")
            row = cur.fetchone()
            current = int(row[0]) if row else 0

    for version, fn in MIGRATIONS:
        if version <= current:
            continue
        try:
            with get_db() as conn:
                with conn.cursor() as cur:
                    fn(cur)
                    cur.execute(
                        "INSERT INTO settings (key, value) VALUES ('db_version', %s)"
                        " ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                        (str(version),),
                    )
            write_log_entry(None, 'db', f'Миграция {version} применена', level='silent')
        except Exception as e:
            write_log_entry(None, 'db', f'Ошибка миграции {version}: {e}', level='silent')
            raise
