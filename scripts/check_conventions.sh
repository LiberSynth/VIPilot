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

# ── Конвенция 2: прямая запись в БД-логи в обход write_log_entry ──────────────
FOUND=$(grep -rn --include="*.py" "db_insert_log_entry" $PROJECT_DIRS \
    | grep -v "^log/log\.py:" \
    | grep -v "^db/")
[ -n "$FOUND" ] && fail "Конвенция 2: db_insert_log_entry вне log/log.py" "$FOUND"

# ── Конвенция 3: защищённые ключи окружения — только в environment.py ─────────
FOUND=$(grep -rn --include="*.py" \
    "db_get(['\"]deep_debugging\|db_get(['\"]loop_interval\|db_get(['\"]max_batch_threads" \
    $PROJECT_DIRS \
    | grep -v "^common/environment\.py:")
[ -n "$FOUND" ] && fail "Конвенция 3: прямое db_get() для ключей окружения вне environment.py" "$FOUND"

FOUND=$(grep -rn --include="*.py" \
    "env_get(['\"]emulation_mode\|env_get(['\"]use_donor" \
    $PROJECT_DIRS \
    | grep -v "^common/environment\.py:" \
    | grep -v "^routes/api\.py:")
[ -n "$FOUND" ] && fail "Конвенция 3: прямое env_get() для ключей окружения вне environment.py" "$FOUND"

# ── Конвенция 6: UUID/GUID в логах — только через fmt_id_msg ─────────────────
# Ищем строки в log/write_log-вызовах, где batch_id или story_id вставлены
# напрямую в f-строку без fmt_id_msg.
FOUND=$(grep -rn --include="*.py" \
    -E "(write_log_entry|write_log|db_log_update)\b.*f['\"].*\{(batch_id|story_id|donor_id|publisher_id)\}" \
    $PROJECT_DIRS \
    | grep -v "fmt_id_msg")
[ -n "$FOUND" ] && fail "Конвенция 6: UUID вставлен напрямую в лог (использовать fmt_id_msg)" "$FOUND"

# ── Конвенция 7: защищённые переменные не читать напрямую в потоках ───────────
# Только пайплайны-потоки (не planning.py — он в главном потоке)
THREAD_PIPELINES="pipelines/story.py pipelines/video.py pipelines/transcode.py pipelines/publish.py"
FOUND=$(grep -rn --include="*.py" \
    "environment\.\(emulation_mode\|use_donor\|deep_logging\|loop_interval\|max_threads\)" \
    $THREAD_PIPELINES 2>/dev/null)
[ -n "$FOUND" ] && fail "Конвенция 7: прямое environment.* в потоке (использовать snap.*)" "$FOUND"

# ── Конвенция 7: все run(batch_id, log_id) должны вызывать snapshot() ─────────
for f in $THREAD_PIPELINES; do
    if [ -f "$f" ] && ! grep -q "environment\.snapshot()" "$f"; then
        fail "Конвенция 7: $f не вызывает environment.snapshot() в run()" ""
    fi
done

# ── Конвенция: статус батча только через db_set_batch_status ─────────────────
# UPDATE batches SET status = 'literal' — нарушение (db_set_batch_status использует %s)
FOUND=$(grep -rn --include="*.py" "SET status = '" $PROJECT_DIRS \
    | grep -v "UPDATE log SET status")
[ -n "$FOUND" ] && fail "Конвенция: прямой SET status с литералом (использовать db_set_batch_status)" "$FOUND"

# INSERT INTO batches с явным полем status — нарушение (DB default = 'pending')
FOUND=$(grep -rn --include="*.py" "INSERT INTO batches" $PROJECT_DIRS \
    | grep "\bstatus\b")
[ -n "$FOUND" ] && fail "Конвенция: INSERT INTO batches с явным полем status (убрать, DB default = 'pending')" "$FOUND"

# ── Итог ──────────────────────────────────────────────────────────────────────
echo ""
if [ "$ERRORS" -eq 0 ]; then
    echo "[OK] Все конвенции соблюдены."
    exit 0
else
    echo "Коммит заблокирован: найдено нарушений — $ERRORS."
    exit 1
fi
