# Конвенция: overlay при публикации (Playwright)

Единственная модель для Dzen / Rutube / VK Video.

---

## Принцип

**Target-first:** dismiss только если **ожидаемый элемент шага не готов** (не найден / не виден / центр перекрыт) **и** по классовым признакам виден блокирующий overlay.

**Whitelist** — только **ожидаемый рабочий UI** пайплайна (captcha, confirm, форма шага). Без каталогов промо/онбординга.

```
wait_for_publish_target / safe_click:
  whitelist handlers (captcha, confirm)
  → цель не готова?
  → publish_overlay_visible() (scrim, dialog, backdrop+контент, popover)
  → dismiss_overlay_strict
```

`handle_popups` **не** вызывает dismiss сам по себе (`allow_dismiss=False` по умолчанию).

---

## Whitelist — что можно

Только **detect** штатного UI текущего шага:

| Имя (пример) | Смысл |
|--------------|--------|
| `captcha` | Капча — отдельный handler |
| `confirm` | Диалог подтверждения публикации |
| `upload_modal` | Модал выбора файла |
| `upload_in_progress` | Идёт загрузка файла после set_files, до URL редактора |
| `upload_form` / `publish_editor` | Форма / редактор публикации |
| `upload_menu` / `create_menu` | Меню «+» / «Загрузить видео» |
| `publish_modal` | Модал «Опубликовать» (VK) |

`handle=None` — «узнали UI, не закрываем».

---

## Запрещено

1. **Списки попапов / туров / хинтов** для dismiss.
2. **Записи мусора в whitelist** — onboarding, hint, toast, tour, popup, overlay и т.п.
3. **Ignore-list chrome дашборда** — промо, онбординг не перечислять; только target-first + generic overlay detect.
4. **Проактивный dismiss** без провала цели шага.

---

## Как закрывать мусор

1. **Триггер:** `publish_target_needs_dismiss(target)` — цель `None`, не видна или `element_obstructed`.
2. **Detect overlay:** `publish_overlay_visible()` **или** цель перекрыта (`element_obstructed`) — без списков промо/текстов.
3. **Dismiss:** `dismiss_overlay_strict` (backdrop → свободная область → alert → Escape → ×); `is_present` — overlay или перекрытие цели.
4. **safe_click:** whitelist → click → при провале `try_dismiss_publish_overlay(target=locator)`.

---

## Исключение: Dzen helper-tooltip

`dismiss_dzen_hint` — стабильный селектор `[class*='helper-tooltip__closeButton']`, вызывается в `before_poll` / `_dzen_dismiss_unknown`, без click-outside.

---

## Проверка

`bash scripts/check_conventions.sh` → `scripts/check_publish_overlay.py`.
