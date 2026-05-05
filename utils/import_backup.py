import json
import os

import psycopg2
import yaml


def _connect():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg2.connect(database_url, connect_timeout=10)


def _get_table_meta(conn, table):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
              AND data_type != 'bytea'
            ORDER BY ordinal_position
        """, (table,))
        cols_info = cur.fetchall()
        columns = [row[0] for row in cols_info]
        jsonb_cols = {row[0] for row in cols_info if row[1] == 'jsonb'}

        cur.execute("""
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema  = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_schema  = 'public'
              AND tc.table_name   = %s
            LIMIT 1
        """, (table,))
        pk_row = cur.fetchone()
        pk = pk_row[0] if pk_row else None

    return columns, pk, jsonb_cols


def _to_db(val, col, jsonb_cols):
    if col in jsonb_cols and isinstance(val, (dict, list)):
        return json.dumps(val, ensure_ascii=False)
    return val


def import_table(table, yaml_content):
    records = yaml.safe_load(yaml_content)
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError(f"YAML для таблицы «{table}» должен содержать список")

    conn = _connect()
    try:
        columns, pk, jsonb_cols = _get_table_meta(conn, table)
        if not columns:
            return {"inserted": 0, "updated": 0, "deleted": 0}

        quoted_cols = [f'"{c}"' for c in columns]
        quoted_pk = f'"{pk}"' if pk else None

        inserted = updated = deleted = 0

        with conn.cursor() as cur:
            if pk:
                yaml_map = {
                    str(rec[pk]): rec
                    for rec in records
                    if rec.get(pk) is not None
                }

                cur.execute(f'SELECT {quoted_pk} FROM "{table}"')
                db_pks = {str(row[0]) for row in cur.fetchall()}

                for pk_val, rec in yaml_map.items():
                    values = [_to_db(rec.get(c), c, jsonb_cols) for c in columns]
                    if pk_val not in db_pks:
                        placeholders = ", ".join(["%s"] * len(columns))
                        cur.execute(
                            f'INSERT INTO "{table}" ({", ".join(quoted_cols)}) '
                            f'VALUES ({placeholders})',
                            values,
                        )
                        inserted += 1
                    else:
                        set_clause = ", ".join(f"{qc} = %s" for qc in quoted_cols)
                        cur.execute(
                            f'UPDATE "{table}" SET {set_clause} WHERE {quoted_pk} = %s',
                            values + [pk_val],
                        )
                        updated += 1

                for db_pk in db_pks:
                    if db_pk not in yaml_map:
                        cur.execute(
                            f'DELETE FROM "{table}" WHERE {quoted_pk} = %s',
                            (db_pk,),
                        )
                        deleted += 1
            else:
                cur.execute(f'TRUNCATE "{table}"')
                for rec in records:
                    values = [_to_db(rec.get(c), c, jsonb_cols) for c in columns]
                    placeholders = ", ".join(["%s"] * len(columns))
                    cur.execute(
                        f'INSERT INTO "{table}" ({", ".join(quoted_cols)}) '
                        f'VALUES ({placeholders})',
                        values,
                    )
                    inserted += 1

        conn.commit()
    finally:
        conn.close()

    return {"inserted": inserted, "updated": updated, "deleted": deleted}
