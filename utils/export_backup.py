import datetime
import decimal
import io
import os
import uuid

import psycopg2
import yaml

class _LiteralStr(str):
    pass

class _BackupDumper(yaml.Dumper):
    pass

def _literal_representer(dumper, data):
    return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")

def _str_representer(dumper, data):
    return dumper.represent_scalar("tag:yaml.org,2002:str", str(data))

_BackupDumper.add_representer(_LiteralStr, _literal_representer)
_BackupDumper.add_representer(uuid.UUID, _str_representer)
_BackupDumper.add_representer(decimal.Decimal, lambda d, v: d.represent_float(float(v)))
_BackupDumper.add_representer(datetime.datetime, lambda d, v: d.represent_scalar("tag:yaml.org,2002:str", v.isoformat()))
_BackupDumper.add_representer(datetime.date, lambda d, v: d.represent_scalar("tag:yaml.org,2002:str", v.isoformat()))

def _coerce(val):
    if isinstance(val, str) and "\n" in val:
        return _LiteralStr(val.replace("\r\n", "\n").replace("\r", "\n"))
    return val

def _get_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)
        return [row[0] for row in cur.fetchall()]

def _get_columns(conn, table):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
              AND data_type != 'bytea'
            ORDER BY ordinal_position
        """, (table,))
        return [row[0] for row in cur.fetchall()]

def _connect():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg2.connect(database_url, connect_timeout=10)

def list_tables():
    conn = _connect()
    try:
        return _get_tables(conn)
    finally:
        conn.close()

def export_table(table):
    conn = _connect()
    try:
        columns = _get_columns(conn, table)
        if not columns:
            records = []
        else:
            cols_sql = ", ".join(f'"{c}"' for c in columns)
            with conn.cursor() as cur:
                cur.execute(f'SELECT {cols_sql} FROM "{table}"')
                rows = cur.fetchall()
            records = [
                {col: _coerce(val) for col, val in zip(columns, row)}
                for row in rows
            ]
    finally:
        conn.close()

    buf = io.StringIO()
    yaml.dump(
        records,
        buf,
        Dumper=_BackupDumper,
        allow_unicode=True,
        default_flow_style=False,
        sort_keys=False,
    )
    return buf.getvalue()
