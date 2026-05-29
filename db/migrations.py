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

from pathlib import Path

from .connection import get_db
from log.log import write_log_entry

_VIDEO_DIR = Path(__file__).resolve().parents[1] / "video"


# ---------------------------------------------------------------------------
# Функции миграций
# ---------------------------------------------------------------------------

def _m1001_drop_targets_legacy_columns(cur):
    """Drop legacy target columns no longer used by stage-batches."""
    cur.execute("ALTER TABLE targets DROP COLUMN IF EXISTS aspect_ratio_x")
    cur.execute("ALTER TABLE targets DROP COLUMN IF EXISTS aspect_ratio_y")
    cur.execute("ALTER TABLE targets DROP COLUMN IF EXISTS transcode")


def _m1002_add_movies_transcoded(cur):
    """Add movies.transcoded with backfill based on transcoded_data files."""
    cur.execute("ALTER TABLE movies ADD COLUMN IF NOT EXISTS transcoded BIT(1)")
    cur.execute("SELECT id::text FROM movies WHERE transcoded IS NULL")
    missing_rows = [str(r[0]) for r in cur.fetchall()]
    for movie_id in missing_rows:
        has_transcoded_file = (_VIDEO_DIR / f"{movie_id} - transcoded_data.mp4").exists()
        cur.execute(
            "UPDATE movies SET transcoded = %s::bit(1) WHERE id = %s::uuid",
            ("1" if has_transcoded_file else "0", movie_id),
        )
    cur.execute("ALTER TABLE movies ALTER COLUMN transcoded SET DEFAULT B'0'")
    cur.execute("ALTER TABLE movies ALTER COLUMN transcoded SET NOT NULL")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_movies_transcoded ON movies (transcoded)")
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_movies_transcode_pick ON movies (used, transcoded, created_at, id)"
    )


# ---------------------------------------------------------------------------
# Реестр миграций — добавляйте только в конец, никогда не переиспользуйте номера
# ---------------------------------------------------------------------------

MIGRATIONS = [
    (2026052901, _m1001_drop_targets_legacy_columns),
    (2026052902, _m1002_add_movies_transcoded),
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
