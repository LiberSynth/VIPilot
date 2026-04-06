# VisionForge — автопубликатор

Flask-приложение для автоматической публикации видеоисторий в VK/Дзен через 6-пайплайновую систему.

## Архитектура пайплайнов

Каждый пайплайн запускается отдельным потоком с интервалом `loop_interval` секунд:

1. **planning.py** — создаёт батчи по расписанию (`schedule`), формирует `batches` со статусом `pending`
2. **story.py** — генерирует текстовый сюжет через OpenRouter → статус `story_ready`
3. **video.py** — запрашивает генерацию видео через fal.ai → статус `video_pending` → `video_ready`
4. **transcode.py** — перекодирует видео через ffmpeg (H.264/AAC/faststart) → статус `transcode_ready`
5. **publish.py** — публикует в VK (история/стена) и/или Дзен → статус `published` (с проверкой времени публикации)
6. **cleanup.py** — удаляет устаревшие log_entries, log, batches/stories, файлы на диске

Статусы батча: `pending → story_ready → video_pending → video_ready → transcode_ready → published`; `отменён` может быть выставлен на любом этапе.

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

## Настройки (settings)

Активные:
- `buffer_hours` — временно́е окно до публикации для попадания в буфер
- `loop_interval` — интервал опроса пайплайнов (секунды)
- `metaprompt` — дополнительный контекст для генерации сюжетов
- `system_prompt` — системный промпт для LLM
- `video_duration` — длительность видео (секунды, 1–60)
- `vk_publish_story` / `vk_publish_wall` — типы публикации в VK
- `emulation_mode` — режим эмуляции (без реальных запросов)
- `log_lifetime` — подробный лог устаревает через N дней (default 30)
- `short_log_lifetime` — краткий лог устаревает через N дней (default 365)
- `batch_lifetime` — история батчей устаревает через N дней (default 7)
- `file_lifetime` — видеофайлы на диске устаревают через N дней (default 7)

Отложено до реализации:
- `notify_email` / `notify_phone` — уведомления при сбоях (функционал будет восстановлен позже)

## Секреты

- `FAL_API_KEY` — ключ fal.ai для генерации видео
- `OPENROUTER_API_KEY` — ключ OpenRouter для генерации сюжетов
- `VK_USER_TOKEN` — личный токен VK
- `DZEN_OAUTH_TOKEN` — OAuth-токен Яндекса (живёт 12 месяцев, не требует обновления)
- `DZEN_CSRF_TOKEN` — CSRF-токен Дзен (резервный, приоритет у targets.config['csrf_token']); срок жизни неизвестен (Яндекс не публикует), обновляется вручную через DevTools
- `ADMIN_PASSWORD` — пароль панели администратора
- `DATABASE_URL` — строка подключения PostgreSQL

## Файлы

- `main.py` — Flask app, регистрация blueprints, middleware, main_loop, старт
- `utils/consts.py` — константы (ADMIN_PASSWORD, FLASK_SECRET, MSK)
- `utils/utils.py` — общие функции (parse_hhmm, to_msk, to_utc_from_msk, parse_history_days, parse_short_log_days)
- `utils/auth.py` — аутентификация (password_fingerprint, is_authenticated)
- `routes/admin.py` — Blueprint: веб-роуты (/, /admin, /save, /save-dzen, /logout, /favicon, /healthz)
- `routes/api.py` — Blueprint: API-роуты (/api/*); модели через db_get/activate/reorder (таблица ai_models)
- `clients/vk.py` — VK API-клиент (publish_story, publish_wall, is_configured)
- `clients/dzen.py` — Дзен API-клиент: 7-шаговый цикл (validate→draft→start-upload→TUS→finish→poll→publish), OAuth + CSRF
- `pipelines/planning.py` — MSK импортируется из utils.consts
- `pipelines/video.py` — video_duration читается в run(), передаётся в _build_body()
- `pipelines/transcode.py` — os.makedirs вызывается внутри run()
- `pipelines/publish.py` — публикация в VKontakte и Дзен
- `pipelines/` — planning.py, story.py, video.py, transcode.py, publish.py, cleanup.py
- `db/connection.py` — единственная точка создания соединения (`get_db()`)
- `db/migrations.py` — реестр `MIGRATIONS` + `run_migrations()`; каждая миграция idempotent и завершается в своей транзакции; версия хранится в `settings.db_version`
- `db/seed.py` — начальные данные (`seed_db()`), только `INSERT … ON CONFLICT DO NOTHING`
- `db/init.py` — тонкий оркестратор: `_bootstrap()` → `run_migrations()` → `seed_db()`
- `db/db.py` — все функции работы с БД; использует `get_db` из `db.connection`
- `db/upgrade.py` — заглушка (обратная совместимость), `init_db()` теперь покрывает всё
- `db/__init__.py` — реэкспорт всех публичных символов
- `log.py` — логирование в БД
- `templates/admin.html` — HTML-скелет панели администратора (5 вкладок); только разметка и Jinja-переменные, без инлайн-CSS и JS
- `templates/login.html` — страница входа
- `static/admin.css` — все стили панели администратора (~850 строк)
- `static/js/ui.js` — навигация: sidebar, switchPanel, часы монитора
- `static/js/workflow.js` — управление движком: start/pause/restart/emulation, showToast, диалоги
- `static/js/settings.js` — автосохранение настроек: сюжет, видео, публикация, служебные; счётчики символов; AR-контрол; валидация lifetime
- `static/js/monitor.js` — монитор: renderLogItem, renderBatch, renderSysGroup, buildTimeline, refreshMonitor
- `static/js/models.js` — списки моделей (видео и текст): drag-and-drop, activate, grade cycle
- `static/js/schedule.js` — управление расписанием: CRUD слотов, runNow
- `static/js/modals.js` — модалки: video, probe (text/video с poll), story

## Запуск

Workflow "Start application": `python main.py`
