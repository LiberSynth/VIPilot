#!/usr/bin/env python3
"""Convention 4: no logging wrapper functions; no app_log."""
from __future__ import annotations

import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCAN_DIRS = ("common", "db", "log", "pipelines", "routes", "utils", "clients", "services")
EXTRA_FILES = ("main.py",)
SKIP_FILES = frozenset({"log/log.py"})
ALLOWED_FUNCTIONS = frozenset({"log_request"})
LOG_CALLS = frozenset({"write_log_entry"})
BUILTIN_CALLS = frozenset(
    {
        "len",
        "str",
        "int",
        "bool",
        "getattr",
        "setattr",
        "isinstance",
        "type",
        "print",
        "range",
        "max",
        "min",
        "sorted",
        "enumerate",
        "zip",
        "any",
        "all",
        "super",
        "open",
    }
)


def rel(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def iter_py_files() -> list[Path]:
    files: list[Path] = []
    for name in SCAN_DIRS:
        base = ROOT / name
        if base.is_dir():
            files.extend(sorted(base.rglob("*.py")))
    for name in EXTRA_FILES:
        path = ROOT / name
        if path.is_file():
            files.append(path)
    return files


def callee_name(node: ast.Call) -> str | None:
    func = node.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


def is_log_wrapper(func: ast.FunctionDef, file_rel: str) -> str | None:
    if func.name in ALLOWED_FUNCTIONS:
        return None
    if file_rel in SKIP_FILES:
        return None

    if func.name == "app_log" or func.name.startswith(("log_app_", "log_batch_", "log_system_")):
        return f"запрещённое имя {func.name!r}"

    if func.name.startswith("log_") and func.name != "log_request":
        return f"def log_* вне log/log.py: {func.name!r}"

    if _is_fmt_id_msg_only_helper(func):
        return (
            f"оболочка сообщения для лога {func.name!r} "
            "(inline fmt_id_msg в write_log_entry)"
        )

    if _is_thin_log_delegate(func):
        return f"оболочка логирования {func.name!r} (inline write_log_entry)"

    return None


def _is_fmt_id_msg_only_helper(func: ast.FunctionDef) -> bool:
    if not func.name.startswith("_"):
        return False
    if len(func.body) > 4:
        return False
    returns_fmt = False
    other_calls = False
    for node in ast.walk(func):
        if isinstance(node, ast.FunctionDef) and node is not func:
            continue
        if isinstance(node, ast.Call):
            name = callee_name(node)
            if name == "fmt_id_msg":
                returns_fmt = True
            elif name not in BUILTIN_CALLS and name not in LOG_CALLS:
                other_calls = True
    return returns_fmt and not other_calls


def _is_thin_log_delegate(func: ast.FunctionDef) -> bool:
    log_calls = 0
    other_calls = 0
    for node in ast.walk(func):
        if isinstance(node, ast.FunctionDef) and node is not func:
            continue
        if not isinstance(node, ast.Call):
            continue
        name = callee_name(node)
        if name in LOG_CALLS:
            log_calls += 1
        elif name not in BUILTIN_CALLS:
            other_calls += 1
    return log_calls > 0 and other_calls == 0


def check_file(path: Path) -> list[str]:
    file_rel = rel(path)
    if file_rel in SKIP_FILES:
        return []
    try:
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(path))
    except SyntaxError as exc:
        return [f"{file_rel}:{exc.lineno or 0}: syntax error: {exc.msg}"]

    violations: list[str] = []
    for node in tree.body:
        if isinstance(node, ast.FunctionDef):
            reason = is_log_wrapper(node, file_rel)
            if reason:
                violations.append(f"{file_rel}:{node.lineno}: {reason}")
    return violations


def main() -> int:
    violations: list[str] = []
    for path in iter_py_files():
        violations.extend(check_file(path))

    if violations:
        for line in violations:
            print(line)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
