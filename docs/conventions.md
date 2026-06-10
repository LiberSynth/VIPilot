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

## Правило 2: Прерывающие ошибки пайплайна → `AppException`

Если внутри пайплайна произошла ситуация, при которой дальнейшая обработка батча невозможна, бросай `AppException`:

```python
from exceptions import AppException

raise AppException(batch_id, pipeline_name, "Описание проблемы")
```

`AppException` перехватывается в `_batch_thread`, переводит батч в статус `error` и записывает это в лог. Это предпочтительнее паттерна «if ошибка: log; return».

---

## Правило 3: Некритичные ошибки → `write_log_entry` с уровнем `warn`/`error`

Ошибки, которые не прерывают пайплайн (предупреждения, частичные сбои), логируются через `write_log_entry` с уровнем `'warn'` или `'error'`:

```python
write_log_entry(batch_id, category, "Не удалось получить превью", level='warn')
write_log_entry(batch_id, category, "Ошибка при загрузке субтитров", level='error')
```

При уровнях `'error'` и `'fatal_error'` функция автоматически вызывает `notify_failure`. В конце потока пайплайна рекомендуется проверить наличие ошибок и отправить итоговое уведомление администратору.

---

## Правило 4: Все логи через `write_log_entry`. Уровень `silent` — только `print()`, без записи в БД

Прямые вызовы `print()` в приложении запрещены. Единая точка записи:

```python
write_log_entry(batch_id, channel, message, level='info')
```

- `batch_id` — UUID батча; `None` — блок «Приложение» в мониторе (окно без батча).
- `channel` — канал записи (`api`, `planning`, `story`, `runner`, `video`, …); хранится в `log_entries.channel`. Префиксы `[tag]` в тексте запрещены — `write_log_entry` переносит известный `tag` в `channel`.
- Уровни: `info`, `warn`, `error`, `fatal_error`, `silent`.
- **Логи «Приложение»** — только через `app_log(source, message, *, level='silent', phase=None)` из `log/log.py`. Прямой `write_log_entry(None, …)` вне `log/log.py` запрещён.
- `app_log(..., level='silent')` — диагностика: stdout всегда; в БД только при `deep_debugging`.
- `app_log(..., level='info')` — действия пользователя и lifecycle (видны в мониторе без deep debug).
- `write_log_entry` автоматически добавляет `.` в конец обычных фраз; не трогает служебные хвосты (`phase=`, `key=value`, URL, `…`, уже есть `.!?`).
- `source` — из фиксированного набора каналов (`main`, `main_loop`, `api`, `http`, `db`, …); для «Приложение» пишется в `log_entries.channel` через `app_log`, без `[source]` в тексте.
- Диагностика пайплайнов вне батча: `app_log('planning', '…', phase='run_start')` → `phase=run_start, …` в сообщении.

Таблица `log` — одна строка на `batch_id` (или окно «Приложение» при `batch_id IS NULL`); канал и текст — в `log_entries` (`channel`, `message`, `level`).

**Запрещено:** прямой SQL к `log` / `log_entries` вне `db_*` с assert-флагами; функции `write_log`, `log_batch_planned`, `log_system_event`, `db_log_update` удалены.

**Оболочки для логирования запрещены.** В месте события вызывай `app_log(...)` или `write_log_entry(...)` напрямую — без промежуточных функций, которые скрывают канал, уровень или текст записи. Примеры запрещённых оболочек:

- `log_app_started()` / `log_app_stopped()` и любые `log_app_*()` → inline `app_log("main", …)`; dedup lifecycle — флаг `_lifecycle_stop_logged` под `_system_log_lock`, сам `app_log` **вне** lock (как в `main.py` при старте).
- `_runner_msg(...)` и аналоги → inline `fmt_id_msg(...)` в аргументе `write_log_entry`.
- Любая `def log_*()` вне `log/log.py`, кроме явно перечисленных исключений ниже.

**Допустимые исключения (не оболочки):**

- `log/log.py` — реализация (`app_log`, `write_log_entry`, `_stdout_log`, `_ensure_log_period`, …).
- `utils/middleware.py::log_request` — Flask `before_request`; фильтрует URL и вызывает `app_log` в том же теле функции.

**Единственное допустимое исключение для `print()`:**
- `print()` внутри самого `log/log.py` — единственная точка вывода в stdout.

**Модификация `log`:** INSERT/UPDATE только при `environment.asserted_log_modification = True` (выставляется внутри `log/log.py`). DELETE — без assert.

---

## Правило 5: Нарушения инвариантов приложения вне пайплайна → `FatalError`

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

Первая строка любого `run(batch_id, category)` в пайплайне:

```python
snap = environment.snapshot()
```

Далее поток использует только значения `snap.*`. Прямое обращение к `environment.*` внутри потока запрещено — иначе окружение может измениться посередине работы потока при следующем тике.

---

## Правило 8: GUID/UUID в лог-сообщениях → `fmt_id_msg` (строгое исполнение)

Все лог-сообщения и строки ошибок, содержащие GUID или UUID-идентификатор (batch_id, story_id, donor_batch_id, publisher_id и т.п.), **обязаны** использовать функцию `fmt_id_msg` из `utils.utils`.

**Прямая вставка или обрезка идентификаторов запрещены:**

```python
# ЗАПРЕЩЕНО — обрезка
write_log_entry(batch_id, category, f"Батч {batch_id[:8]}…")
write_log_entry(batch_id, category, f"publisher={publisher_id[:12]}…")

# ЗАПРЕЩЕНО — прямая вставка без fmt_id_msg
write_log_entry(batch_id, category, f"Батч {batch_id}")
```

**Правильно — через `fmt_id_msg`:**

```python
from utils.utils import fmt_id_msg

# Один идентификатор
write_log_entry(batch_id, category, fmt_id_msg("Батч {} начало", batch_id))

# Несколько идентификаторов — позиционно
write_log_entry(batch_id, category, fmt_id_msg("Батч {} — донор {}", batch_id, donor_batch_id))

# В строке ошибки
msg = fmt_id_msg("Ошибка записи story_id={} в БД", story_id)
write_log_entry(batch_id, category, msg, level='error')
raise AppException(batch_id, category, msg)
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
| Некритичная ошибка / предупреждение | `write_log_entry(batch_id, category, msg, level='warn'/'error')` |
| Информационное сообщение | `write_log_entry(batch_id, category, msg)` |
| Лог блока «Приложение» (диагностика) | `app_log(source, msg, level='silent')` или `phase='…'` |
| Лог блока «Приложение» (операторское) | `app_log(source, msg, level='info')` |
| Оболочка вокруг `app_log` / `write_log_entry` | **Запрещено** — inline в месте события |
| Нарушение инварианта приложения вне пайплайна | `raise FatalError(msg)` |
| Критическое исключение (не глушить) | Дать всплыть до `_batch_thread` |
| GUID/UUID в лог-сообщении | `fmt_id_msg(template, *ids)` из `utils.utils` |
| Чтение переменных окружения | Только в `refresh_environment()`, далее через `environment.*` или `snap.*` |
| Чтение окружения внутри потока | `snap = environment.snapshot()` — первая строка `run()` |
| Вставка в миграции | `INSERT ... SELECT ... WHERE NOT EXISTS (...)` |
| DDL в миграции | `ADD COLUMN IF NOT EXISTS`, `DROP COLUMN IF EXISTS`, `CREATE INDEX IF NOT EXISTS` и т.д. |
| Диалог подтверждения или модал в JS | `new ConfirmDialog({...}).open()` или класс, наследующий `Dialog` |
| Стили `<textarea>` | Тег-селектор в `common.css`; инлайн `style` и `min-height` запрещены |
