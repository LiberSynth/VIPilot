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
  4. Ориентир: удалять не раньше двух недель после деплоя на prod.
     Дата деплоя указана в docstring каждой миграции (поле «Deployed:»).

Пример: _m001 задеплоена 2026-01-01, db_version в prod = 3.
  → Через две недели удаляем функцию и строку (1, _m001) из MIGRATIONS.
  → MIGRATIONS начинается с (2, _m002) — это нормально, пробел в нумерации
    не влияет на работу.
══════════════════════════════════════════════════════════════════════════
"""

from .connection import get_db
from log.log import write_log_entry


# ---------------------------------------------------------------------------
# Функции миграций
# ---------------------------------------------------------------------------

# Миграции 1–102 удалены.


# Миграции 103 удалена.


def _m104_ai_models_grade_nullable(cur):
    """
    Убирает NOT NULL и DEFAULT 'good' у колонки grade в ai_models.
    Deployed: -
    """
    cur.execute("ALTER TABLE ai_models ALTER COLUMN grade DROP NOT NULL")
    cur.execute("ALTER TABLE ai_models ALTER COLUMN grade DROP DEFAULT")


# ---------------------------------------------------------------------------
# Реестр миграций — добавляйте только в конец, никогда не переиспользуйте номера
# ---------------------------------------------------------------------------

def _m105_batches_type_status_created_at(cur):
    """
    batches.type → TEXT, убирает NOT NULL и DEFAULT 'slot'.
    batches.status → TEXT.
    batches.created_at → DEFAULT now().
    Deployed: -
    """
    cur.execute("ALTER TABLE batches ALTER COLUMN type TYPE TEXT")
    cur.execute("ALTER TABLE batches ALTER COLUMN type DROP NOT NULL")
    cur.execute("ALTER TABLE batches ALTER COLUMN type DROP DEFAULT")
    cur.execute("ALTER TABLE batches ALTER COLUMN status TYPE TEXT")
    cur.execute("ALTER TABLE batches ALTER COLUMN created_at SET DEFAULT now()")


MIGRATIONS = [
    (104, _m104_ai_models_grade_nullable),
    (105, _m105_batches_type_status_created_at),
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
            write_log_entry(None, f"[DB] Миграция {version} применена", level='silent')
        except Exception as e:
            write_log_entry(None, f"[DB] Ошибка миграции {version}: {e}", level='silent')
            raise
