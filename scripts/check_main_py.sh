#!/bin/bash
# Сигнал: main.py изменён без явного одобрения пользователя.
# MAIN_PY_LINE_COUNT — зафиксированный baseline, не норма из конвенций.
# После осмотра diff: откатите main.py или обновите MAIN_PY_LINE_COUNT здесь.

MAIN_PY_LINE_COUNT=105

MAIN_PY_LINES=$(wc -l < main.py | tr -d ' ')
if [ "$MAIN_PY_LINES" -eq "$MAIN_PY_LINE_COUNT" ]; then
    exit 0
fi

echo ""
echo "[FAIL] Файл main.py изменён: сейчас $MAIN_PY_LINES строк, в scripts/check_main_py.sh зафиксировано $MAIN_PY_LINE_COUNT."
echo "Просмотрите diff. Откатите изменения или обновите MAIN_PY_LINE_COUNT в scripts/check_main_py.sh."
exit 1
