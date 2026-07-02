# CONVENTIONS (Combined)

Этот файл объединяет обязательные конвенции проекта из:

- `docs/conventions.md`
- `docs/project-conventions.md`
- `docs/upgrade-convention.md`
- `.cursor/rules/project-conventions.mdc`

---

## Source: `docs/conventions.md`

# Конвенция обработки ошибок и логирования

Этот документ фиксирует принятые соглашения для единообразной обработки ошибок и логирования во всём приложении.

---

## Правило 1: Не глушить критические исключения

Исключения, которые делают дальнейшую логику невозможной или некорректной, **не должны перехватываться и замалчиваться** внутри пайплайна или бизнес-логики. Такие ошибки должны «всплывать» в `_batch_thread` в `main.py`, где они обрабатываются централизованно — переводят батч в статус `fatal_error` и отправляют уведомление.

**Не делай так:**
```python
try:
    do_something_critical()
except Exception:
    pass  # глушим — ЗАПРЕЩЕНО
```

---

## Правило 2: Прерывающие ошибки пайплайна -> `AppException`

Если внутри пайплайна произошла ситуация, при которой дальнейшая обработка батча невозможна, бросай `AppException`:

```python
from exceptions import AppException

raise AppException(batch_id, pipeline_name, "Описание проблемы")
```

`AppException` перехватывается в `_batch_thread`, переводит батч в статус `error` и записывает это в лог. Это предпочтительнее паттерна «if ошибка: log; return».

---

## Правило 3: Некритичные ошибки -> `log_entry` с уровнем `warn`/`error`

Ошибки, которые не прерывают пайплайн (предупреждения, частичные сбои), логируются через `log_entry` или `pipeline_log` с уровнем `'warn'` или `'error'`:

```python
log_entry(log_id, "Не удалось получить превью", level='warn')
pipeline_log(log_id, "Ошибка при загрузке субтитров", level='error')
```

При уровнях `'error'` и `'fatal_error'` функция автоматически вызывает `notify_failure`. В конце потока пайплайна рекомендуется проверить наличие ошибок и отправить итоговое уведомление администратору.

---

## Правило 4: Все инфо-логи через `log_entry` / `pipeline_log`. Уровень `silent` — только `print()`, без записи в БД

Прямые вызовы `print()` в приложении запрещены. Используй:

- `log_entry(log_id, message, level='info')` — для информационных записей в лог (уровни: `info`, `warn`, `error`, `fatal_error`).
- `pipeline_log(log_id, message, level='info')` — аналог внутри пайплайнов (уровни: `info`, `warn`, `error`).
- `log_entry(None, message, level='silent')` — для диагностических сообщений, которые **не должны попадать в БД**: только `print()` в stdout, без DB-записи, без `notify_failure`.
- `pipeline_log(None, message, level='silent')` — аналог для пайплайнов.

**Единственное допустимое исключение:**
- `print()` внутри самого `log/log.py` — единственная точка вывода в stdout.

---

## Правило 5: Нарушения инвариантов приложения вне пайплайна -> `FatalError`

Если обнаружена ситуация, нарушающая базовый инвариант всего приложения, и эта ситуация **не привязана к конкретному батчу или пайплайну** — используй `FatalError`:

```python
from exceptions import FatalError

raise FatalError("Неизвестный статус батча 'xyz' — добавь его в KNOWN_BATCH_STATUSES")
```

`FatalError` не является `AppException` (нет `batch_id`/`pipeline`) и не перехватывается штатным обработчиком пайплайна. Она всплывает как непойманное исключение и переводит батч в `fatal_error` через общий обработчик `except Exception` в `_batch_thread`.

---

## Правило 6: Переменные окружения — только через `refresh_environment()` и `EnvSnapshot`

Переменные `deep_logging`, `loop_interval`, `max_threads` читаются из БД **исключительно** в `refresh_environment()`. Прямые вызовы `db_get`/`env_get` для этих ключей в любом другом месте — запрещены.

`refresh_environment()` вызывается в начале каждого тика главного цикла и атомарно заменяет `environment._current` на новый `EnvSnapshot`. Модульные переменные (`environment.loop_interval` и т.д.) обновляются синхронно для кода главного потока.

---

## Правило 7: Потоки читают окружение только через снимок, захваченный один раз

Первая строка любого `run(batch_id, log_id)` в пайплайне:

```python
snap = environment.snapshot()
```

Далее поток использует только значения `snap.*`. Прямое обращение к `environment.*` внутри потока запрещено — иначе окружение может измениться посередине работы потока при следующем тике.

---

## Правило 8: GUID/UUID в лог-сообщениях -> `fmt_id_msg` (строгое исполнение)

Все лог-сообщения и строки ошибок, содержащие GUID или UUID-идентификатор (batch_id, story_id, donor_batch_id, publisher_id и т.п.), **обязаны** использовать функцию `fmt_id_msg` из `utils.utils`.

**Прямая вставка или обрезка идентификаторов запрещены:**

```python
# ЗАПРЕЩЕНО — обрезка
pipeline_log(log_id, f"Батч {batch_id[:8]}…")
pipeline_log(log_id, f"publisher={publisher_id[:12]}…")

# ЗАПРЕЩЕНО — прямая вставка без fmt_id_msg
pipeline_log(log_id, f"Батч {batch_id}")
```

**Правильно — через `fmt_id_msg`:**

```python
from utils.utils import fmt_id_msg

# Один идентификатор
pipeline_log(log_id, fmt_id_msg("Батч {} начало", batch_id))

# Несколько идентификаторов — позиционно
pipeline_log(log_id, fmt_id_msg("Батч {} — донор {}", batch_id, donor_batch_id))

# В строке ошибки
msg = fmt_id_msg("Ошибка записи story_id={} в БД", story_id)
pipeline_log(log_id, msg, level='error')
raise AppException(batch_id, 'story', msg)
```

Функция `fmt_id_msg(template: str, *ids) -> str` подставляет каждый переданный идентификатор **целиком** (без обрезки, без `…`) в соответствующий `{}` плейсхолдер шаблона.

---

## Правило 9: Каждая SQL-инструкция в миграции должна быть идемпотентной

Миграции запускаются при каждом старте приложения. Любая SQL-инструкция внутри миграции **обязана** корректно отрабатывать при повторном запуске — без ошибок и без создания дублей.

### Вставка строк

Единственный допустимый паттерн — `WHERE NOT EXISTS`:

```sql
-- ПРАВИЛЬНО
INSERT INTO ai_platforms (name, url, env_key_name)
SELECT 'Grok', 'https://api.x.ai/v1', 'XAI_API_KEY'
WHERE NOT EXISTS (SELECT 1 FROM ai_platforms WHERE name = 'Grok');
```

`ON CONFLICT DO NOTHING` **запрещён** как инструмент идемпотентности вставок, если на целевом столбце нет `UNIQUE`-ограничения. Без уникального индекса эта конструкция не срабатывает и запись вставляется повторно при каждом прогоне.

```sql
-- ЗАПРЕЩЕНО (нет UNIQUE на name — конфликт никогда не возникнет)
INSERT INTO ai_platforms (name, url, env_key_name)
VALUES ('Grok', 'https://api.x.ai/v1', 'XAI_API_KEY')
ON CONFLICT DO NOTHING;
```

Исключение: `ON CONFLICT DO NOTHING` допустим только если на целевом столбце или комбинации столбцов уже существует `UNIQUE`-индекс или первичный ключ.

### DDL-инструкции

Используй встроенные идемпотентные варианты:

```sql
ALTER TABLE t ADD COLUMN IF NOT EXISTS col TEXT;
ALTER TABLE t DROP COLUMN IF EXISTS col;
CREATE INDEX IF NOT EXISTS idx_name ON t(col);
DROP INDEX IF EXISTS idx_name;
CREATE TABLE IF NOT EXISTS t (...);
DROP TABLE IF EXISTS t;
```

### Перед тем как написать новую миграцию

Открой `db/migrations.py` и найди аналогичную инструкцию, уже написанную ранее. Сверься с её паттерном — он уже проверен и идемпотентен.

---

## Правило 10: Все JS-диалоги и модалы наследуются от `Dialog`

Любой диалог подтверждения или модальное окно в клиентском JavaScript **обязано** быть реализовано через классовую иерархию из `static/js/controls/`:

- **`Dialog`** (`controls/dialog.js`) — базовый класс: управляет ловушкой фокуса (Tab/Shift+Tab), закрытием по Esc, восстановлением фокуса на `triggerBtn` после закрытия.
- **`ConfirmDialog extends Dialog`** (`controls/confirm-dialog.js`) — готовый диалог подтверждения с заголовком, текстом и кнопками. Принимает `{ title, text, confirmLabel, cancelLabel, confirmStyle, onConfirm(btn, dlg), onCancel, triggerBtn }`.
- Полноценные модалы (видео, зонд, сюжет и т.п.) **расширяют `Dialog`** и переопределяют `open()`, `close()` и `onClose()` под свою специфику.

**Прямое создание DOM-элементов с классом `confirm-overlay` запрещено** — только через `new ConfirmDialog({...}).open()` или наследника `Dialog`.

Оба файла подключаются в `templates/root.html` и `templates/production.html` **перед** остальными скриптами приложения, что делает `Dialog` и `ConfirmDialog` доступными глобально.

---

## Правило 11: Стили `<textarea>` — через тег-селектор, без инлайна и класса

Все многострочные поля ввода стилизуются через тег-селектор `textarea` в `static/common.css`. Дополнительный CSS-класс не нужен.

- Базовые стили (фон, рамка, отступы, шрифт, `resize`, `min-height`) — только в блоке `textarea { … }` в `common.css`.
- Индивидуальная высота — через ID-селектор в `common.css` (`#ta { height: 300px; }`).
- Инлайновые атрибуты `style` на `<textarea>` **запрещены**, кроме `width`/`box-sizing`/`resize`, если они нужны для конкретного контекста вёрстки (например, readonly-блок в карточке).
- `min-height` в инлайне **запрещён** — базовое значение уже задано в `common.css`.

---

## Сводная таблица

| Ситуация | Механизм |
|---|---|
| Критическая ошибка внутри пайплайна (прерывает батч) | `raise AppException(batch_id, pipeline, msg)` |
| Некритичная ошибка / предупреждение | `log_entry(log_id, msg, level='warn'/'error')` |
| Информационное сообщение | `log_entry(log_id, msg)` или `pipeline_log(log_id, msg)` |
| Диагностическое сообщение (только stdout, не в БД) | `log_entry(None, msg, level='silent')` |
| Нарушение инварианта приложения вне пайплайна | `raise FatalError(msg)` |
| Критическое исключение (не глушить) | Дать всплыть до `_batch_thread` |
| GUID/UUID в лог-сообщении | `fmt_id_msg(template, *ids)` из `utils.utils` |
| Чтение переменных окружения | Только в `refresh_environment()`, далее через `environment.*` или `snap.*` |
| Чтение окружения внутри потока | `snap = environment.snapshot()` — первая строка `run()` |
| Вставка в миграции | `INSERT ... SELECT ... WHERE NOT EXISTS (...)` |
| DDL в миграции | `ADD COLUMN IF NOT EXISTS`, `DROP COLUMN IF EXISTS`, `CREATE INDEX IF NOT EXISTS` и т.д. |
| Диалог подтверждения или модал в JS | `new ConfirmDialog({...}).open()` или класс, наследующий `Dialog` |
| Стили `<textarea>` | Тег-селектор в `common.css`; инлайн `style` и `min-height` запрещены |

---

## Source: `docs/project-conventions.md`

# VIPilot — Автоматическая публикация видеоисторий

Flask-приложение для автоматической публикации видеоисторий в VK, Дзен и Рутьюб через систему из шести пайплайнов.

## Run & Operate

*   **Run application:** `python main.py`
*   **Env vars:** `FAL_API_KEY`, `OPENROUTER_API_KEY`, `VK_USER_TOKEN`, `DZEN_OAUTH_TOKEN`, `DATABASE_URL`
*   **Post-task check:** Always run `bash scripts/check_conventions.sh` before reporting task completion. Fix any violations.

## Stack

*   **Framework:** Flask
*   **ORM:** _Populate as you build_
*   **Validation:** _Populate as you build_
*   **Build Tool:** _Populate as you build_
*   **Runtime:** Python

## Where things live

*   **Pipelined Workflow:** `pipelines/` (modules: `planning.py`, `story.py`, `video.py`, `transcode.py`, `publish.py`, `cleanup.py`)
*   **Web Routes:** `routes/web.py` (frontend), `routes/api.py` (backend API)
*   **Database Schema:** Defined implicitly by `db/migrations.py` and `db/seed.py`
*   **Configuration:** `settings` and `cycle_config` tables in DB via `db/cycle_config.py`
*   **AI Models:** `ai_models` table in DB, managed by `routes/api.py`
*   **Logging:** `log/log.py`
*   **Frontend Assets:** `static/` (CSS, JS, images)
*   **HTML Templates:** `templates/` (Jinja2)
*   **Core Application Logic:** `main.py`, `common/`, `utils/`
*   **API Clients:** `clients/` (`dzen.py`, `rutube.py`, `vkvideo.py`, `common.py`)
*   **Browser Automation Services:** `services/` (`browser_base.py`, `browser_registry.py`, `publish_batch_browser.py`)

## Architecture decisions

*   **Pipeline-based processing:** A 6-stage pipeline (planning, story, video, transcode, publish, cleanup) handles video story generation and publication, with each stage running in its own thread.
*   **Centralized error handling:** Custom `AppException` and `FatalError` classes are used for predictable and invariant-breaking errors, caught by the main loop.
*   **Strict environment access:** Environment variables and settings are read into a snapshot at the beginning of each pipeline run to ensure consistency and prevent race conditions.
*   **Browser automation for complex platforms:** Dzen and RuTube (and VK Video) publication is handled via Playwright-driven browser automation due to complex UI interactions.
*   **Single point of DB access:** All database interactions are routed through `db/db.py` to ensure consistent connection management and transaction handling.
*   **All JS dialogs and modals inherit from `Dialog`.**
*   **Styling for `<textarea>` is done via tag-selector in `common.css`, not inline or via class.**

## Product

*   Automated scheduling and batching of video story generation.
*   AI-powered text story generation (OpenRouter) and video generation (fal.ai).
*   Video transcoding for platform compatibility (ffmpeg).
*   Multi-platform publishing to VK (stories/wall), Dzen, and RuTube.
*   Monitoring interface for pipeline status and logs.
*   User-configurable production cycle settings (prompts, duration, approval flows).

## User preferences

- **Strict adherence to conventions:** `scripts/check_conventions.sh` must pass before reporting task completion. Disabling or bypassing the `pre-commit` hook is forbidden.
- **Error handling in pipelines:** Exceptions should bubble up; direct `try-except` around DB operations is forbidden **inside pipelines**.
- **Logging:** Use `write_log` / `write_log_entry` exclusively; direct `print()` or direct DB writes to logs are forbidden.
- **Environment variables:** Read via `refresh_environment()` only; direct `db_get`/`env_get` for specific keys outside this function is forbidden.
- **Thread environment:** Threads must read environment settings from a snapshot (`snap = environment.snapshot()`).
- **GUID/UUID formatting:** Use `fmt_id_msg` for all GUID/UUID in log messages.

## Gotchas

*   `pre-commit` hook: The `.git/hooks/pre-commit` hook is mandatory and should never be disabled or bypassed. Violations must be fixed.
*   Direct DB access: Do not directly access environment variables or settings from the DB; use `environment.*` or `snap.*`.
*   Logging bypass: Do not use `print()` for logging or directly write to log tables; use the provided `log/log.py` functions.
*   `notify_failure` exceptions: `notify_failure` handles its own exceptions; do not wrap its calls in external `try-except` blocks.

## Pointers

*   **Code Conventions:** `docs/conventions.md`
*   **Flask Documentation:** [https://flask.palletsprojects.com/](https://flask.palletsprojects.com/)
*   **OpenRouter API:** [https://openrouter.ai/docs](https://openrouter.ai/docs)
*   **Fal.ai API:** [https://docs.fal.ai/](https://docs.fal.ai/)
*   **FFmpeg Documentation:** [https://ffmpeg.org/documentation.html](https://ffmpeg.org/documentation.html)
*   **Playwright Documentation:** [https://playwright.dev/python/docs/intro](https://playwright.dev/python/docs/intro)

---

## Source: `docs/upgrade-convention.md`

# Конвенция: пост-апгрейд процедура

## Правило

Все проверки серверного окружения и установка зависимостей — **по умолчанию исключительно в `upgrade.py`**.

Допустимое исключение: runtime-fallback для Chromium в `pipelines/base.py`
(`ensure_playwright_chromium`) перед браузерной публикацией, если бинарник
отсутствует на хосте.

## Что относится к «проверкам окружения»

- Наличие системных бинарников (ffmpeg, ffprobe, chromium)
- Наличие Python-пакетов (flask, requests, psycopg2, playwright)
- Установка браузерных зависимостей (playwright install chromium)

## Как работает `check_upgrade()`

Признак первого запуска — отсутствие `build_number` в таблице `environment`.
`seed_db()` вызывается **только при первом запуске**. `build_number` фиксируется после всех шагов.

### Во всех средах

1. `bootstrap()` — idempotent.
2. Если `build_number` совпадает с `BUILD` — ранний выход, ничего не делается.
3. `_run_env_checks()` — проверка окружения (бросает `RuntimeError` при неудаче).
4. Если `build_number` отсутствует → `seed_db()` (users-семейство).
5. `run_migrations()` -> `_run_post_migration_checks()`.
6. `env_set(build_number)` — фиксируется **только после** успешного завершения всех шагов.
7. Возвращает `True` при выполнении апгрейда, `False` при раннем выходе.

Примечание по `pg_repack`:
- На Windows отсутствие `pg_repack` CLI допускается (дефрагментация идёт через `VACUUM FULL`).
- Для строгой проверки на Windows задаётся `VIPILOT_REQUIRE_PG_REPACK=1`.

## Что делать если нужна новая проверка окружения

Добавить функцию `_check_<name>() -> tuple[bool, str]` в `upgrade.py`
и зарегистрировать её в списке `_CHECKS`.

## Что делать если нужна новая пост-миграционная проверка данных

Добавить функцию `_check_<name>() -> tuple[bool, str]` в `upgrade.py`
и зарегистрировать её в списке `_POST_MIGRATION_CHECKS`.

## Что не нужно делать

- **Не устанавливать** зависимости в сервисах (например, в `browser_base.py`).
  Если Chromium отсутствует — сервис падает с ошибкой от Playwright. Это ожидаемое поведение:
  при следующем деплое `upgrade.py` установит его до запуска сервисов.
- **Не добавлять** новые установки зависимостей в воркфлоу/сервисы/пайплайны.
  Разрешено только указанное выше исключение для `ensure_playwright_chromium`.
- **Не вызывать** `run_upgrades` или аналогичные функции из роутов или воркфлоу.
- **Не дублировать** проверки окружения в нескольких местах.

---

## Source: `.cursor/rules/project-conventions.mdc`

```md
---
description: Always apply FlaskApp source conventions
alwaysApply: true
---

# FlaskApp Conventions

In this repository, always treat these files as mandatory guidance:

- `docs/conventions.md`
- `docs/upgrade-convention.md`
- `docs/project-conventions.md`

Required behavior:

- Apply all three documents in every chat/session for this repo.
- Follow their standards for error handling, logging, environment access, and UI/backend patterns.
- If any rule is ambiguous or conflicting, ask the user before implementing.
- After substantial code edits, run `bash scripts/check_conventions.sh` when available and fix reported violations.

## Hard Execution Gate (User Controlled)

- Default mode is analysis-only. Do not edit files, run modifying shell commands, or perform git write actions unless the user explicitly authorizes execution in the current message.
- Forbidden without explicit user command in the current message:
  - Any file modifications (`ApplyPatch`, notebook edits, file delete/create).
  - Any git write operations (`git add`, `git commit`, `git push`, branch changes that modify state).
  - Any command with side effects on project code or data.
- Before any modification, ask for explicit confirmation with this exact phrase:
  - `Готов выполнить. Подтвердите командой: Выполняй.`
- Commit is allowed only on the explicit user command:
  - `Комит`
- If the user says `ничего не делать`, `стоп`, `только обсудить`, or equivalent, perform read-only analysis only.
```
