#!/usr/bin/env python3
"""Publish overlay convention: whitelist only, no popup catalogs."""
from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CLIENTS = ROOT / "clients"
TARGETS = ("dzen.py", "rutube.py", "vkvideo.py")

ALLOWED_WHITELIST_NAMES = frozenset({
    "captcha",
    "confirm",
    "create_menu",
    "file_input",
    "publish_editor",
    "publish_modal",
    "publish_workflow",
    "upload_form",
    "upload_in_progress",
    "upload_processing",
    "upload_menu",
    "upload_modal",
})

FORBIDDEN_WHITELIST_NAMES = frozenset({
    "onboarding",
    "tour",
    "hint",
    "toast",
    "popup",
    "overlay",
    "coach",
    "tooltip",
    "garbage",
    "publish_hint",
    "save_error_toast",
    "save_error",
    "modal",
})

FORBIDDEN_CONST_RE = re.compile(
    r"_(ONBOARDING|POPUP|TOUR|GARBAGE|HINT_TEXTS|TOAST_TEXTS|SAVE_ERROR_TOAST)[A-Z0-9_]*\s*=",
)

FORBIDDEN_DEF_RE = re.compile(
    r"def _(detect|handle)_[a-z0-9]*(onboarding|tour|hint|toast|popup)[a-z0-9_]*\(",
    re.IGNORECASE,
)

FORBIDDEN_FUNC_RE = re.compile(
    r"def _[a-z0-9]*(onboarding_visible|tour_step|popup_list)[a-z0-9_]*\(",
    re.IGNORECASE,
)


def rel(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def check_file(path: Path) -> list[str]:
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()
    errors: list[str] = []

    for i, line in enumerate(lines, 1):
        if m := FORBIDDEN_CONST_RE.search(line):
            errors.append(f"{rel(path)}:{i}: запрещён каталог попапов {m.group(0).rstrip('=')}")
        if FORBIDDEN_DEF_RE.search(line):
            errors.append(f"{rel(path)}:{i}: detect/handle мусорного попапа в whitelist-паттерне")
        if FORBIDDEN_FUNC_RE.search(line):
            errors.append(f"{rel(path)}:{i}: функция каталога попапов")

    try:
        tree = ast.parse(text, filename=str(path))
    except SyntaxError as exc:
        errors.append(f"{rel(path)}: syntax error: {exc}")
        return errors

    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if not isinstance(target, ast.Name):
                continue
            if not target.id.endswith("_PUBLISH_WHITELIST"):
                continue
            if not isinstance(node.value, ast.List):
                continue
            for elt in node.value.elts:
                if not isinstance(elt, ast.Tuple) or len(elt.elts) < 1:
                    continue
                name_node = elt.elts[0]
                if not isinstance(name_node, ast.Constant) or not isinstance(name_node.value, str):
                    continue
                name = name_node.value
                if name in FORBIDDEN_WHITELIST_NAMES:
                    errors.append(
                        f"{rel(path)}:{name_node.lineno}: мусор {name!r} в whitelist "
                        "(только рабочий UI; мусор → dismiss_overlay_strict)",
                    )
                elif name not in ALLOWED_WHITELIST_NAMES:
                    errors.append(
                        f"{rel(path)}:{name_node.lineno}: неизвестная запись whitelist {name!r} "
                        f"(допустимо: {', '.join(sorted(ALLOWED_WHITELIST_NAMES))})",
                    )
    return errors


def main() -> int:
    errors: list[str] = []
    for name in TARGETS:
        path = CLIENTS / name
        if path.is_file():
            errors.extend(check_file(path))
    if errors:
        print("Publish overlay convention violations:", file=sys.stderr)
        for err in errors:
            print(f"  {err}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
