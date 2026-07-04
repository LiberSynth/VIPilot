#!/bin/bash
# Автоматическая проверка конвенций VIPilot
# Запускается как pre-commit хук и вручную: bash scripts/check_conventions.sh

ERRORS=0

# Директории проекта (без кешей и зависимостей)
PROJECT_DIRS="common db log pipelines routes static utils clients"

fail() {
    echo ""
    echo "[FAIL] $1"
    [ -n "$2" ] && echo "$2"
    ERRORS=$((ERRORS + 1))
}

# ── Конвенция 2: print() запрещён вне log/log.py ─────────────────────────────
FOUND=$(grep -rn --include="*.py" "^\s*print(" $PROJECT_DIRS \
    | grep -v "^log/log\.py:")
[ -n "$FOUND" ] && fail "Конвенция 2: print() вне log/log.py" "$FOUND"

# ── Конвенция 2: прямая запись в log_entries в обход write_log_entry ──────────────
FOUND=$(grep -rn --include="*.py" "db_insert_log_entry" $PROJECT_DIRS \
    | grep -v "^log/log\.py:" \
    | grep -v "^db/")
[ -n "$FOUND" ] && fail "Конвенция 2: db_insert_log_entry вне log/log.py" "$FOUND"

# ── Конвенция 2: прямая модификация log (INSERT/UPDATE) вне log/log.py ─────────
FOUND=$(grep -rn --include="*.py" -E "(INSERT INTO log|UPDATE log SET)" $PROJECT_DIRS \
    | grep -v "^log/log\.py:" \
    | grep -v "^db/db_simple\.py:" \
    | grep -v "^db/migrations\.py:" \
    | grep -v "^db/init\.py:")
[ -n "$FOUND" ] && fail "Конвенция 2: прямой INSERT/UPDATE log вне log/log.py и db_simple" "$FOUND"

# ── Конвенция 4: app_log и оболочки логирования запрещены ─────────────────────
FOUND=$(grep -rn --include="*.py" -E "\bapp_log\b" $PROJECT_DIRS services main.py \
    | grep -v "^scripts/check_log_wrappers\.py:")
[ -n "$FOUND" ] && fail "Конвенция 4: app_log запрещён (inline write_log_entry)" "$FOUND"

# ── Конвенция 4: оболочки логирования запрещены (см. docs/conventions.md) ────
_run_python() {
    if command -v python >/dev/null 2>&1 && python -c "import ast" >/dev/null 2>&1; then
        python "$@"
        return $?
    fi
    if command -v py >/dev/null 2>&1 && py -3 -c "import ast" >/dev/null 2>&1; then
        py -3 "$@"
        return $?
    fi
    if command -v python3 >/dev/null 2>&1 && python3 -c "import ast" >/dev/null 2>&1; then
        python3 "$@"
        return $?
    fi
    return 127
}
WRAPPER_RC=0
WRAPPER_OUT=$(_run_python scripts/check_log_wrappers.py 2>&1) || WRAPPER_RC=$?
if [ "$WRAPPER_RC" -eq 127 ]; then
    fail "Конвенция 4: python не найден для scripts/check_log_wrappers.py" ""
elif [ "$WRAPPER_RC" -ne 0 ]; then
    fail "Конвенция 4: оболочки логирования вне log/log.py" "$WRAPPER_OUT"
fi

# ── Конвенция 3: защищённые ключи окружения — только в environment.py ─────────
FOUND=$(grep -rn --include="*.py" \
    "db_get(['\"]deep_debugging\|db_get(['\"]loop_interval\|db_get(['\"]max_batch_threads" \
    $PROJECT_DIRS \
    | grep -v "^common/environment\.py:")
[ -n "$FOUND" ] && fail "Конвенция 3: прямое db_get() для ключей окружения вне environment.py" "$FOUND"

# ── Конвенция 6: UUID/GUID в логах — только через fmt_id_msg ─────────────────
# Ищем строки в log/write_log-вызовах, где batch_id или story_id вставлены
# напрямую в f-строку без fmt_id_msg.
FOUND=$(grep -rn --include="*.py" \
    -E "write_log_entry\b.*f['\"].*\{(batch_id|story_id|donor_id|publisher_id)\}" \
    $PROJECT_DIRS \
    | grep -v "fmt_id_msg")
[ -n "$FOUND" ] && fail "Конвенция 6: UUID вставлен напрямую в лог (использовать fmt_id_msg)" "$FOUND"

# ── Конвенция 7: защищённые переменные не читать напрямую в потоках ───────────
# Только пайплайны-потоки (не planning.py — он в главном потоке)
THREAD_PIPELINES="pipelines/story.py pipelines/video.py pipelines/transcode.py pipelines/publish.py"
FOUND=$(grep -rn --include="*.py" \
    "environment\.\(deep_logging\|loop_interval\|max_threads\)" \
    $THREAD_PIPELINES 2>/dev/null)
[ -n "$FOUND" ] && fail "Конвенция 7: прямое environment.* в потоке (использовать snap.*)" "$FOUND"

# ── Конвенция 7: все run(batch_id, category) должны вызывать snapshot() ───────
for f in $THREAD_PIPELINES; do
    if [ -f "$f" ] && ! grep -q "environment\.snapshot()" "$f"; then
        fail "Конвенция 7: $f не вызывает environment.snapshot() в run()" ""
    fi
done

# ── Конвенция: статус батча только через db_set_batch_status ─────────────────
# UPDATE batches SET status = 'literal' — нарушение (db_set_batch_status использует %s)
FOUND=$(grep -rn --include="*.py" "SET status = '" $PROJECT_DIRS)
[ -n "$FOUND" ] && fail "Конвенция: прямой SET status с литералом (использовать db_set_batch_status)" "$FOUND"

# INSERT INTO batches с явным полем status — нарушение (DB default = 'pending')
FOUND=$(grep -rn --include="*.py" "INSERT INTO batches" $PROJECT_DIRS \
    | grep "\bstatus\b")
[ -n "$FOUND" ] && fail "Конвенция: INSERT INTO batches с явным полем status (убрать, DB default = 'pending')" "$FOUND"

# ── main.py: сигнал несанкционированного изменения ───────────────────────────
if ! bash scripts/check_main_py.sh; then
    ERRORS=$((ERRORS + 1))
fi

# ── Конвенция publish overlay: whitelist only, no popup catalogs ─────────────
OVERLAY_RC=0
OVERLAY_OUT=$(_run_python scripts/check_publish_overlay.py 2>&1) || OVERLAY_RC=$?
if [ "$OVERLAY_RC" -eq 127 ]; then
    fail "Publish overlay: python не найден для scripts/check_publish_overlay.py" ""
elif [ "$OVERLAY_RC" -ne 0 ]; then
    fail "Publish overlay (docs/publish-overlay-convention.md)" "$OVERLAY_OUT"
fi

# ── Итог ──────────────────────────────────────────────────────────────────────
echo ""
if [ "$ERRORS" -eq 0 ]; then
    echo "[OK] Все конвенции соблюдены."
    exit 0
else
    echo "Коммит заблокирован: найдено нарушений — $ERRORS."
    exit 1
fi
