import json
import os
import sys

import psycopg2
import yaml


SECTIONS = [
    {
        "name": "settings",
        "fields": ["key", "value"],
    },
    {
        "name": "environment",
        "fields": ["key", "value"],
    },
    {
        "name": "cycle_config",
        "fields": ["key", "value"],
        "block_text_fields": ["value"],
    },
    {
        "name": "ai_platforms",
        "fields": ["id", "name", "url", "env_key_name"],
    },
    {
        "name": "ai_models",
        "fields": ["id", "platform_id", "name", "url", "body", "type", "price", "order", "active", "grade", "note"],
        "jsonb_fields": ["body"],
    },
    {
        "name": "model_durations",
        "fields": ["id", "model_id", "duration"],
    },
    {
        "name": "targets",
        "fields": [
            "id", "name", "aspect_ratio_x", "aspect_ratio_y",
            "active", "order", "transcode", "config", "slug",
        ],
        "jsonb_fields": ["config"],
    },
    {
        "name": "users",
        "fields": ["id", "name", "login", "password"],
    },
    {
        "name": "user_roles",
        "fields": ["id", "name", "slug", "module"],
    },
    {
        "name": "user_role_links",
        "fields": ["id", "user_id", "role_id"],
    },
]


class _LiteralStr(str):
    pass


class _PackageDumper(yaml.Dumper):
    pass


def _literal_representer(dumper, data):
    return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")


_PackageDumper.add_representer(_LiteralStr, _literal_representer)


def _build_row(section, col_names, row_values):
    jsonb_fields = set(section.get("jsonb_fields", []))
    block_text_fields = set(section.get("block_text_fields", []))
    record = {}
    for col, val in zip(col_names, row_values):
        if col in jsonb_fields:
            if isinstance(val, str):
                val = json.loads(val)
        elif col in block_text_fields:
            if isinstance(val, str) and "\n" in val:
                val = _LiteralStr(val.replace("\r\n", "\n").replace("\r", "\n"))
        if isinstance(val, memoryview):
            val = bytes(val)
        record[col] = val
    return record


def export(output_path="update_package.yaml", stream=None):
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        sys.stderr.write("ERROR: DATABASE_URL is not set\n")
        sys.exit(1)

    conn = psycopg2.connect(database_url, connect_timeout=10)
    try:
        cur = conn.cursor()
        package = {}

        for section in SECTIONS:
            table = section["name"]
            fields = section["fields"]
            cols = ", ".join(f'"{f}"' for f in fields)
            cur.execute(f"SELECT {cols} FROM {table}")
            rows = cur.fetchall()
            records = [_build_row(section, fields, row) for row in rows]
            package[table] = records

        cur.close()
    finally:
        conn.close()

    if stream is not None:
        yaml.dump(
            package,
            stream,
            Dumper=_PackageDumper,
            allow_unicode=True,
            default_flow_style=False,
            sort_keys=False,
        )
    else:
        with open(output_path, "w", encoding="utf-8") as f:
            yaml.dump(
                package,
                f,
                Dumper=_PackageDumper,
                allow_unicode=True,
                default_flow_style=False,
                sort_keys=False,
            )
        sys.stdout.write(f"Exported to {output_path}\n")


if __name__ == "__main__":
    export()
