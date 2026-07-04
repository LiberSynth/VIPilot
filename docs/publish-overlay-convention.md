# Конвенция: overlay при публикации (Playwright)

Единственная модель для Dzen / Rutube / VK Video.

---

## Принцип

**Whitelist** — только **ожидаемый рабочий UI** пайплайна публикации (то, что **нельзя** закрывать).

**Всё остальное — мусор.** Закрывается **одной** цепочкой `dismiss_overlay_strict` (снаружи → Escape → ×), без каталога попапов.

```
handle_popups(whitelist) → если не whitelist → dismiss_unknown → dismiss_overlay_strict
```

---

## Whitelist — что можно

Только **detect** штатного UI текущего шага:

| Имя (пример) | Смысл |
|--------------|--------|
| `captcha` | Капча — отдельный handler |
| `confirm` | Диалог подтверждения публикации |
| `upload_modal` | Модал выбора файла |
| `upload_form` / `publish_editor` | Форма / редактор публикации |
| `upload_menu` / `create_menu` | Меню «+» / «Загрузить видео» |
| `upload_in_progress` | Идёт загрузка файла |
| `publish_modal` | Модал «Опубликовать» (VK) |

`handle=None` — «узнали UI, не закрываем».

---

## Запрещено

1. **Списки попапов / туров / хинтов** для dismiss: `_ONBOARDING_*`, `_POPUP_*`, `_TOUR_*`, `_HINT_TEXTS`, `_TOAST_TEXTS`, перечисления текстов «Новый раздел», «Уже можно публиковать» и т.п.
2. **Записи мусора в whitelist** — `onboarding`, `hint`, `toast`, `tour`, `popup`, `overlay`, `publish_hint`, `save_error_toast` и любые detect+handle «закрыть этот попап».
3. **Отдельные функции** `_onboarding_visible`, `_detect_*_hint`, `_handle_*_tour` **в whitelist**.
4. **Клик по тексту кнопки тура** («Далее», «Пропустить») как стратегия dismiss — только generic close.

---

## Как закрывать мусор

1. **Признак блокировки:** целевой элемент виден, но **не кликается** (`elementFromPoint` / `safe_click`), **или** `_likely_overlay_present`.
2. **Не whitelist** → `dismiss_overlay_strict`.
3. Платформа может передать **generic** `extra_close_selectors` (×, `[aria-label*='Закрыть']`) — **не** тексты конкретных попапов.

---

## Исключение: Dzen helper-tooltip

`dismiss_dzen_hint` — **не whitelist**, а **dismiss_unknown** для одного стабильного селектора `[class*='helper-tooltip__closeButton']`. Без click-outside (ломает меню «+»). Без списков текстов хинтов.

После `handle_popups` в `_dzen_handle_popups` — повторный вызов `dismiss_dzen_hint` (редактор в whitelist блокирует dismiss_unknown).

---

## Detect whitelist — строго

Detect **не должен** матчиться на постоянный chrome студии (пункты меню «Модерация», «Описание» в сайдбаре). Только признаки **открытой формы** текущего шага.

---

## Проверка

`bash scripts/check_conventions.sh` → `scripts/check_publish_overlay.py`.

Коммит с нарушениями в `clients/{dzen,rutube,vkvideo}.py` **блокируется**.
