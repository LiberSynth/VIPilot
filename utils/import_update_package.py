import json
import os

import psycopg2
import yaml


SECTIONS = [
    {"name": "settings",        "fields": ["key", "value"],                                                          "pk": "key"},
    {"name": "environment",     "fields": ["key", "value"],                                                          "pk": "key"},
    {"name": "cycle_config",    "fields": ["key", "value"],                                                          "pk": "key"},
    {"name": "ai_platforms",    "fields": ["id", "name", "url", "env_key_name"],                                     "pk": "id"},
    {"name": "ai_models",       "fields": ["id", "platform_id", "name", "url", "body", "type", "price", "order", "active", "grade", "note"], "pk": "id", "jsonb_fields": ["body"], "full_replace": True},
    {"name": "model_durations", "fields": ["id", "model_id", "duration"],                                            "pk": "id", "full_replace": True},
    {"name": "targets",         "fields": ["id", "name", "aspect_ratio_x", "aspect_ratio_y", "active", "order", "transcode", "config", "slug"], "pk": "id", "jsonb_fields": ["config"]},
    {"name": "users",           "fields": ["id", "name", "login", "password"],                                       "pk": "id"},
    {"name": "user_roles",      "fields": ["id", "name", "slug", "module"],                                          "pk": "id"},
    {"name": "user_role_links", "fields": ["id", "user_id", "role_id"],                                              "pk": "id"},
]


def _to_db(col, val, jsonb_fields):
    if col in jsonb_fields and isinstance(val, (dict, list)):
        return json.dumps(val, ensure_ascii=False)
    return val


def import_package(stream):
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set")

    data = yaml.safe_load(stream)
    if not isinstance(data, dict):
        raise ValueError("Неверный формат YAML")

    summary = {}

    conn = psycopg2.connect(database_url, connect_timeout=10)
    try:
        cur = conn.cursor()

        for section in SECTIONS:
            table = section["name"]
            fields = section["fields"]
            pk = section["pk"]
            jsonb_fields = set(section.get("jsonb_fields", []))
            quoted_fields = [f'"{f}"' for f in fields]
            quoted_pk = f'"{pk}"'

            yaml_records = data.get(table) or []

            if section.get("full_replace"):
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                deleted = cur.fetchone()[0]
                cur.execute(f"DELETE FROM {table}")
                inserted = 0
                placeholders = ", ".join(["%s"] * len(fields))
                for rec in yaml_records:
                    if rec.get(pk) is None:
                        continue
                    values = [_to_db(f, rec.get(f), jsonb_fields) for f in fields]
                    cur.execute(
                        f'INSERT INTO {table} ({", ".join(quoted_fields)}) VALUES ({placeholders})',
                        values,
                    )
                    inserted += 1
                summary[table] = {"inserted": inserted, "updated": 0, "deleted": deleted}
                continue

            yaml_map = {str(rec[pk]): rec for rec in yaml_records if rec.get(pk) is not None}

            cur.execute(f"SELECT {quoted_pk} FROM {table}")
            db_pks = {str(row[0]) for row in cur.fetchall()}

            inserted = updated = deleted = 0

            for pk_val, rec in yaml_map.items():
                values = [_to_db(f, rec.get(f), jsonb_fields) for f in fields]
                if pk_val not in db_pks:
                    placeholders = ", ".join(["%s"] * len(fields))
                    cur.execute(
                        f'INSERT INTO {table} ({", ".join(quoted_fields)}) VALUES ({placeholders})',
                        values,
                    )
                    inserted += 1
                else:
                    set_clause = ", ".join(f"{qf} = %s" for qf in quoted_fields)
                    cur.execute(
                        f"UPDATE {table} SET {set_clause} WHERE {quoted_pk} = %s",
                        values + [pk_val],
                    )
                    updated += 1

            for db_pk in db_pks:
                if db_pk not in yaml_map:
                    cur.execute(f"DELETE FROM {table} WHERE {quoted_pk} = %s", (db_pk,))
                    deleted += 1

            summary[table] = {"inserted": inserted, "updated": updated, "deleted": deleted}

        conn.commit()
        cur.close()
    finally:
        conn.close()

    return summary
