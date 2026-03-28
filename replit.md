# VK Daily Story Publisher

Автоматическая система ежедневной публикации видеоисторий в VK сообщество (-236929597).

## Как работает

1. **04:00 UTC** — система генерирует видео через fal.ai (minimax/video-01)
2. Случайный промпт выбирается из 15 сценариев на тему строительства/ремонта
3. Опрос статуса каждые 30 секунд (генерация занимает ~3-4 минуты)
4. Видео скачивается с автоматическим retry при сбоях
5. Транскодируется через ffmpeg (H.264/AAC/faststart) для VK
6. **Публикуется в историю сообщества** сразу после готовности (после 06:00 UTC или сразу после генерации)

## Технические детали

- **Модель**: `fal-ai/minimax/video-01` (aspect_ratio: 9:16, duration: 6 сек)
- **fal.ai API**: ручной polling через `https://queue.fal.run/fal-ai/minimax/requests/{request_id}/status`
- **Результат**: `GET https://queue.fal.run/fal-ai/minimax/requests/{request_id}` (без sub-path модели — важно!)
- **VK API**: `stories.getVideoUploadServer` → upload → `stories.save`
- **Retry**: при сбое генерации — следующая попытка через 30 минут; при сбое скачивания — 3 попытки

## Секреты

- `FAL_API_KEY` — ключ fal.ai для генерации видео
- `VK_USER_TOKEN` — личный токен VK с правами на истории сообщества

## Запуск

Workflow "Start application" запускает `python main.py` — бесконечный цикл с проверкой каждые 30 сек.

## Файлы

- `main.py` — основной публикатор
- `attached_assets/generated_videos/` — тестовые видео
