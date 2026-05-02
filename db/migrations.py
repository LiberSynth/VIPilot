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

# Миграции 1–92 удалены: задеплоены на prod 2026-04-30, db_version = 92.
# Следующая миграция: _m094_...


def _m093_log_entries_created_at_idx(cur):
    """Индекс по log_entries.created_at для быстрого поиска системных логов по диапазону дат."""
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_log_entries_created_at"
        " ON log_entries (created_at)"
    )


def _m094_datetime_indexes(cur):
    """Индексы по created_at для таблиц ai_models, movies, schedule, stories."""
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ai_models_created_at ON ai_models (created_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_movies_created_at    ON movies    (created_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_schedule_created_at  ON schedule  (created_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_stories_created_at   ON stories   (created_at)")


def _m095_misc_field_indexes(cur):
    """Индексы по часто фильтруемым полям: grade, level, manual_changed, module,
    order, pinned, pipeline, status, title."""
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ai_models_grade       ON ai_models    (grade)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_movies_grade          ON movies       (grade)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_log_entries_level     ON log_entries  (level)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_stories_manual_changed ON stories     (manual_changed)")
    cur.execute('CREATE INDEX IF NOT EXISTS idx_user_roles_module     ON user_roles   (module)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_ai_models_order       ON ai_models    ("order")')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_targets_order         ON targets      ("order")')
    cur.execute("CREATE INDEX IF NOT EXISTS idx_stories_pinned        ON stories      (pinned)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_log_pipeline          ON log          (pipeline)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_log_status            ON log          (status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_batches_title         ON batches      (title)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_stories_title         ON stories      (title)")


# ---------------------------------------------------------------------------
# Реестр миграций — добавляйте только в конец, никогда не переиспользуйте номера
# ---------------------------------------------------------------------------

MIGRATIONS = [
    (93, _m093_log_entries_created_at_idx),
    (94, _m094_datetime_indexes),
    (95, _m095_misc_field_indexes),
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
