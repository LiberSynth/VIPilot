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

# Миграции 1–107 удалены.


def _m108_drop_file_lifetime_setting(cur):
    """Удаляет устаревший параметр settings.file_lifetime."""
    cur.execute("DELETE FROM settings WHERE key = 'file_lifetime'")


def _m109_drop_emulation_mode_environment_key(cur):
    """Удаляет устаревший параметр environment.emulation_mode."""
    cur.execute("DELETE FROM environment WHERE key = 'emulation_mode'")


def _m110_rename_legacy_batch_types_to_manual(cur):
    """Переименовывает legacy типы/статусы батчей в manual."""
    cur.execute("""
        UPDATE batches
           SET type = CASE
               WHEN type = 'movie_probe' THEN 'movie_manual'
               WHEN type = 'story_probe' THEN 'story_manual'
               ELSE type
           END
         WHERE type IN ('movie_probe', 'story_probe')
    """)
    cur.execute("""
        UPDATE batches
           SET status = CASE
               WHEN status = 'movie_probe' THEN 'movie_manual'
               WHEN status = 'story_probe' THEN 'story_manual'
               ELSE status
           END
         WHERE status IN ('movie_probe', 'story_probe')
    """)


# ---------------------------------------------------------------------------
# Реестр миграций — добавляйте только в конец, никогда не переиспользуйте номера
# ---------------------------------------------------------------------------

MIGRATIONS = [
    (108, _m108_drop_file_lifetime_setting),
    (109, _m109_drop_emulation_mode_environment_key),
    (110, _m110_rename_legacy_batch_types_to_manual),
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
