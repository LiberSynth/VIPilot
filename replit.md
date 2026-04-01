# Red Brick Core — автопубликатор

Flask-приложение для автоматической публикации видеоисторий в VK/Дзен через 6-пайплайновую систему.

## Архитектура пайплайнов

Каждый пайплайн запускается отдельным потоком с интервалом `loop_interval` секунд:

1. **planning.py** — создаёт батчи по расписанию (`schedule`), формирует `batches` со статусом `pending`
2. **story.py** — генерирует текстовый сюжет через OpenRouter → статус `story_ready`
3. **video.py** — запрашивает генерацию видео через fal.ai → статус `video_pending` → `video_ready`
4. **transcode.py** — перекодирует видео через ffmpeg (H.264/AAC/faststart) → статус `transcode_ready`
5. **publish.py** — публикует в VK (история/стена) и/или Дзен → статус `published` (с проверкой времени публикации)
6. **cleanup.py** — архивирует старые записи (пока не активирован)

Статусы батча: `pending → story_ready → video_pending → video_ready → transcode_ready → published`; `устарел` может быть выставлен на любом этапе.

## Таблицы БД

- `settings` — ключ/значение настроек
- `schedule` — расписание публикаций (время UTC)
- `targets` — целевые площадки (VK, Дзен) с токенами и параметрами
- `batches` — очередь задач пайплайна (UUID PK, все статусы и данные)
- `stories` — сгенерированные тексты сюжетов
- `ai_models` — модели fal.ai (name, url, body JSONB, order, active)
- `ai_platforms` — платформы ИИ
- `log` — корневые записи лога
- `log_entries` — записи внутри корневых логов
- `video_urls` — URL сгенерированных видео для эмуляции

## Настройки (settings)

Активные:
- `buffer_hours` — временно́е окно до публикации для попадания в буфер
- `loop_interval` — интервал опроса пайплайнов (секунды)
- `metaprompt` — дополнительный контекст для генерации сюжетов
- `system_prompt` — системный промпт для LLM
- `video_duration` — длительность видео (секунды, 1–60)
- `vk_publish_story` / `vk_publish_wall` — типы публикации в VK
- `emulation_mode` — режим эмуляции (без реальных запросов)
- `history_days` — глубина истории в мониторе
- `short_log_days` — срок хранения коротких логов

Отложено до реализации:
- `notify_email` / `notify_phone` — уведомления при сбоях (функционал будет восстановлен позже)

## Секреты

- `FAL_API_KEY` — ключ fal.ai для генерации видео
- `OPENROUTER_API_KEY` — ключ OpenRouter для генерации сюжетов
- `VK_USER_TOKEN` — личный токен VK
- `ADMIN_PASSWORD` — пароль панели администратора
- `DATABASE_URL` — строка подключения PostgreSQL

## Файлы

- `main.py` — Flask + запуск потоков пайплайнов + роуты панели
- `pipelines/` — story.py, video.py, transcode.py, publish.py, planning.py, cleanup.py
- `db/` — init.py, db.py, upgrade.py, __init__.py
- `log.py` — логирование в БД
- `templates/admin.html` — панель администратора (5 вкладок)
- `templates/login.html` — страница входа

## Запуск

Workflow "Start application": `python main.py`
