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

# Миграции 1–100 удалены.


def _m101(cur):
    """Перепривязывает три батча к каноничным сюжетам после ручной чистки дублей.

    Deployed: TBD
    """
    cur.execute(
        "UPDATE batches SET story_id = %s::uuid WHERE id = %s::uuid",
        ('6438ff25-8a29-4d03-afdd-3c22738646ac', '9fefe577-d59c-4463-a533-cc983147cb07'),
    )
    cur.execute(
        "UPDATE batches SET story_id = %s::uuid WHERE id = ANY(%s::uuid[])",
        (
            '3edfd7f1-1fd2-4130-a860-1a6bc3c1edf6',
            [
                '27c55b0b-b1f6-4e67-b60b-26441daddeed',
                '23e6354a-eee2-4a7b-a5c3-11e1e214df5f',
                'e00cf160-5dcc-43a8-9133-e4f1df2c5ea4',
            ],
        ),
    )


# ---------------------------------------------------------------------------
# Реестр миграций — добавляйте только в конец, никогда не переиспользуйте номера
# ---------------------------------------------------------------------------

MIGRATIONS = [
    (101, _m101),
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
