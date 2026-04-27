#!/usr/bin/env bash
# Убирает файлы браузерного кэша из индекса Git.
# Файлы остаются на диске — только прекращают отслеживаться.
# Запускать один раз вручную из корня репозитория.
set -e

git rm -r --cached data/rutube_profile/ data/dzen_profile/
echo "Готово. Теперь выполните git commit -m 'Remove browser cache from tracking'."
