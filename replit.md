# VIPilot — автопубликатор

Flask-приложение для автоматической публикации видеоисторий в VK/Дзен через 6-пайплайновую систему.

## Архитектура пайплайнов

Каждый пайплайн запускается отдельным потоком с интервалом `loop_interval` секунд:

1. **planning.py** — создаёт батчи по расписанию (`schedule`), формирует `batches` со статусом `pending`
2. **story.py** — генерирует текстовый сюжет через OpenRouter → статус `story_ready`
3. **video.py** — запрашивает генерацию видео через fal.ai → статус `video_pending` → `video_ready`
4. **transcode.py** — перекодирует видео через ffmpeg (H.264/AAC/faststart) → статус `transcode_ready`
5. **publish.py** — публикует в VK (история/стена) и/или Дзен → статус `published` (с проверкой времени публикации)
6. **cleanup.py** — удаляет устаревшие log_entries, log, batches/stories, файлы на диске

Статусы батча: `pending → story_ready → video_pending → video_ready → transcode_ready → published`; `cancelled` может быть выставлен на любом этапе ожидания.

Повторяющийся код вынесен в `pipelines/base.py`: `check_cancelled(pipeline_name, batch_id, batch)` — все пайплайны используют эту функцию вместо копипасты. Прерывающие ошибки внутри пайплайнов выбрасывают `AppException` (из `exceptions.py`), которое перехватывает `_batch_thread` в `main.py` и переводит батч в статус `error`. Неизвестные исключения переводят батч в `fatal_error`.

## Таблицы БД

- `settings` — ключ/значение настроек (системные параметры: lifetime, buffer_hours, loop_interval, max_*)
- `cycle_config` — типизированные настройки производственного цикла (одна строка, id=1): text_prompt, format_prompt, video_post_prompt, video_duration, approve_stories, approve_movies
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
- `emulation_mode` — режим эмуляции (без реальных запросов)
- `log_lifetime` — подробный лог устаревает через N дней (default 30)
- `short_log_lifetime` — краткий лог устаревает через N дней (default 365)
- `batch_lifetime` — история батчей устаревает через N дней (default 7)
- `file_lifetime` — видеофайлы на диске устаревают через N дней (default 7)

Настройки производственного цикла перенесены в `cycle_config` (типизированная таблица, см. выше).

Отложено до реализации:
- `notify_email` / `notify_phone` — уведомления при сбоях (функционал будет восстановлен позже)

## Секреты

- `FAL_API_KEY` — ключ fal.ai для генерации видео
- `OPENROUTER_API_KEY` — ключ OpenRouter для генерации сюжетов
- `VK_USER_TOKEN` — личный токен VK
- `DZEN_OAUTH_TOKEN` — OAuth-токен Яндекса (живёт 12 месяцев, не требует обновления)
- `DATABASE_URL` — строка подключения PostgreSQL

## Файлы

- `main.py` — Flask app, регистрация blueprints, middleware, main_loop, старт; при запуске вызывает `db_log_interrupt_running` для всех четырёх пайплайнов (сброс «висячих» running-записей в мониторе)
- `common/exceptions.py` — классы `AppException(batch_id, pipeline, message)` и `FatalError` для прерывающих ошибок
- `common/startup.py` — инициализация приложения (`init_app`, `create_app`)
- `common/statuses.py` — константы статусов батчей (`KNOWN_BATCH_STATUSES`, `FINAL_BATCH_STATUSES` и др.)
- `common/gunicorn.conf.py` — конфиг gunicorn (worker_class, threads, timeout)
- `common/environment.py` — состояние процесса: ContextVar `asserted_log_entry`, слоты потоков батчей, wakeup-событие главного цикла
- `utils/consts.py` — константы (FLASK_SECRET, MSK)
- `utils/utils.py` — общие функции (parse_hhmm, to_msk, to_utc_from_msk, parse_short_log_days)
- `utils/auth.py` — аутентификация (is_authenticated): проверяет session["auth"] и auth_ts (7 дней)
- `routes/web.py` — Blueprint "web": веб-роуты (/, /web, /production, /save, /save-dzen, /logout, /favicon, /healthz)
- `routes/api.py` — Blueprint: API-роуты (/api/*); модели через db_get/activate/reorder (таблица ai_models)
- `clients/vk.py` — VK API-клиент (publish_story, publish_wall, is_configured)
- `clients/dzen.py` — Дзен API-клиент: 7-шаговый цикл (validate→draft→start-upload→TUS→finish→poll→publish), OAuth + CSRF
- `pipelines/base.py` — общие утилиты пайплайнов: `check_cancelled`, `pipeline_log`, `iterate_models`
- `pipelines/planning.py` — MSK импортируется из utils.consts
- `pipelines/story.py` — проверяет возвращаемое значение `db_set_batch_story`; при False — логирует ошибку и завершается через `return`
- `pipelines/video.py` — video_duration читается в run(), передаётся в _build_body()
- `pipelines/transcode.py` — os.makedirs вызывается внутри run()
- `pipelines/publish.py` — публикация в VKontakte и Дзен
- `pipelines/` — base.py, planning.py, story.py, video.py, transcode.py, publish.py, cleanup.py
- `db/cycle_config.py` — доступ к таблице cycle_config: `cycle_config_get()` → dict, `cycle_config_set(**kwargs)` → прямые SQL-запросы
- `db/connection.py` — единственная точка создания соединения (`get_db()`)
- `db/migrations.py` — реестр `MIGRATIONS` + `run_migrations()`; каждая миграция idempotent и завершается в своей транзакции; версия хранится в `settings.db_version`
- `db/seed.py` — начальные данные (`seed_db()`), только `INSERT … ON CONFLICT DO NOTHING`
- `db/init.py` — тонкий оркестратор: `_bootstrap()` → `run_migrations()` → `seed_db()`
- `db/db.py` — все функции работы с БД; использует `get_db` из `db.connection`
- `db/upgrade.py` — пост-апгрейд обработка: проверка окружения и БД при каждом старте (`check_upgrade`)
- `db/__init__.py` — реэкспорт всех публичных символов
- `log/log.py` — логирование: `write_log`, `write_log_entry`, `db_log_update`, `db_get_monitor` и др.
- `docs/conventions.md` — конвенции кода (обработка ошибок, логирование, GUID в логах)
- `templates/root.html` — HTML-скелет панели администратора (5 вкладок); использует common/header.html и макрос info_panel
- `templates/production.html` — HTML-страница роли producer (модуль PRODUCTION); одна вкладка «Информация»
- `templates/login.html` — страница входа (Логин + Пароль)
- `templates/common/header.html` — общий заголовок, подключается через {% include %} в root.html и production.html
- `templates/common/info_panel.html` — Jinja2-макрос info_panel(caption, description, show_developer=False)
- `templates/common/controls.html` — общие UI-контролы: макрос modes_card(approve_stories, use_donor, approve_movies) — карточка «Режимы» (Утверждать сюжеты + Утверждать видео + Подбирать видео из пула); используется в root.html (Рабочий поток) и production.html (Режиссер)
- `static/common.css` — все стили панели администратора (~850 строк)
- `static/js/controls/toast.js` — утилита showToast
- `static/js/controls/tooltip.js` — тач-тултип для мобильных устройств
- `static/js/controls/dialog.js` — базовый класс `Dialog`: ловушка фокуса (Tab/Shift+Tab), Esc, восстановление фокуса; все диалоги и модалы наследуются от него
- `static/js/controls/confirm-dialog.js` — `ConfirmDialog extends Dialog`: диалог подтверждения; принимает `{ title, text, confirmLabel, cancelLabel, confirmStyle, onConfirm(btn, dlg), onCancel, triggerBtn }`
- `static/js/shell.js` — навигация: sidebar, switchPanel, часы монитора (загружается последним)
- `static/js/screenwriter.js` — автосохранение черновика, список сюжетов, кнопка «Сгенерировать»
- `static/js/workflow.js` — управление движком: start/pause/restart/emulation, диалоги
- `static/js/settings.js` — автосохранение настроек: сюжет, видео, публикация, служебные; счётчики символов; AR-контрол; валидация lifetime
- `static/js/monitor.js` — монитор: renderLogItem, renderBatch, renderSysGroup, buildTimeline, refreshMonitor
- `static/js/models.js` — списки моделей (видео и текст): drag-and-drop, activate, grade cycle. Список моделей — единый параметризованный компонент (`renderModelList`). Любое новое отличие в поведении добавляется через параметр в `opts`. Создавать отдельные копии рендерера или ветвить логику через контекстные if-проверки внутри рендерера — запрещено.
- `static/js/schedule.js` — управление расписанием: CRUD слотов, runNow
- `static/js/modals.js` — модалки: video, probe (text/video с poll), story

## Конвенции (обязательно к исполнению, см. полный текст в docs/conventions.md)

**1. Чтение-запись в БД не оборачивается `try-except`.** Исключения всплывают наверх — в главный цикл или раннер потока.

**2. Единственный способ логирования — `write_log` / `write_log_entry`.** Прямой `print()` запрещён по всему проекту. Единственное исключение — внутри самого `log/log.py`. Прямая запись в БД-логи в обход `write_log_entry` запрещена.

**3. Переменные окружения читаются из БД только в `refresh_environment()`.** Прямые `db_get`/`env_get` для ключей `deep_logging`, `emulation_mode`, `use_donor`, `loop_interval`, `max_threads` в любом другом месте запрещены. Доступ — только через `environment.*` или `snap.*`.

**4. `notify_failure` владеет своим `try-except` сама.** Никто снаружи не оборачивает её вызов в `try-except`.

**5. Два типа исключений.** `AppException(batch_id, pipeline, msg)` — прерывание пайплайна по ожидаемой причине. `FatalError(msg)` — нарушение инварианта приложения.

**6. GUID/UUID в лог-сообщениях — только через `fmt_id_msg`.** Прямая вставка или обрезка идентификаторов запрещена.

**7. Потоки читают окружение только через снимок.** Первая строка любого `run(batch_id, log_id)` в пайплайне: `snap = environment.snapshot()`. Далее только `snap.*`, обращение к `environment.*` внутри потока запрещено.

**10. Все JS-диалоги и модалы наследуются от `Dialog`.** `ConfirmDialog extends Dialog` — для диалогов подтверждения; полноценные модалы (video, probe, story) тоже наследуют `Dialog`. Прямое создание DOM-элементов с классом `confirm-overlay` запрещено.

**11. Стили `<textarea>` — через тег-селектор в `common.css`, без инлайна и класса.** Базовые стили — в блоке `textarea { … }`, индивидуальная высота — через ID-селектор. Инлайновый `style` и `min-height` на `<textarea>` запрещены.

## Запуск

Workflow "Start application": `python main.py`

## Обязательно после каждой задачи

Перед тем как сообщить о завершении задачи, всегда запускать: `bash scripts/check_conventions.sh`. Если скрипт возвращает нарушения — исправить их до отчёта.

## ⛔ ЗАПРЕЩЕНО НАВСЕГДА — pre-commit хук

`.git/hooks/pre-commit` подключён к `scripts/check_conventions.sh` и **НИКОГДА не должен отключаться**.

Запрещено:
- Заменять содержимое `.git/hooks/pre-commit` на `exit 0` или любой другой no-op
- Удалять `.git/hooks/pre-commit`
- Обходить хук через `git commit --no-verify`
- Изменять `scripts/check_conventions.sh` так, чтобы он перестал проверять нарушения

Если хук мешает коммиту — это значит, что в коде есть нарушение конвенции. Нарушение нужно исправить, а не отключать хук. Если исправить нарушение сложно — сообщить об этом разработчику и ждать решения. Тихо отключать хук запрещено. Пользователь явно запретил отключение хука.
