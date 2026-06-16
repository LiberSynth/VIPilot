"""
Центральный хаб JPEG-кадров публикации для SSE-трансляции в Монитор.

Пайплайн пушит кадры через push(); клиент Монитора подписывается на stream_generator().
"""

import base64
import threading
import time
from typing import Optional

class PublishFrameHub:
    """Потокобезопасный буфер кадров батча и SSE-генератор для Монитора."""

    def __init__(self):
        self._lock = threading.Lock()
        self._frames: dict[str, tuple[bytes, float, int]] = {}
        self._counters: dict[str, int] = {}
        self._events: dict[str, threading.Event] = {}
        self._stopped: set[str] = set()

    def push(self, batch_id: str, img: bytes) -> None:
        """Сохраняет кадр и будит подписчиков SSE."""
        with self._lock:
            self._stopped.discard(batch_id)
            counter = self._counters.get(batch_id, 0) + 1
            self._counters[batch_id] = counter
            self._frames[batch_id] = (img, time.monotonic(), counter)
            event = self._events.setdefault(batch_id, threading.Event())
        event.set()

    def clear(self, batch_id: str) -> None:
        """Удаляет кадр и сигнализирует подписчикам о завершении."""
        with self._lock:
            self._frames.pop(batch_id, None)
            self._counters.pop(batch_id, None)
            self._stopped.add(batch_id)
            event = self._events.get(batch_id)
        if event is not None:
            event.set()

    def get_frame(self, batch_id: str) -> Optional[bytes]:
        """Возвращает последний JPEG батча или None."""
        with self._lock:
            entry = self._frames.get(batch_id)
            return entry[0] if entry else None

    def is_stopped(self, batch_id: str) -> bool:
        """True если трансляция батча завершена (clear вызван)."""
        with self._lock:
            return batch_id in self._stopped

    def stream_generator(self, batch_id: str):
        """
        SSE-генератор: выдаёт кадры как base64-encoded JPEG.
        Формат: 'data: <base64>\\n\\n', завершение: 'data: STOPPED\\n\\n'.
        """
        last_counter = -1

        while True:
            with self._lock:
                entry = self._frames.get(batch_id)
                stopped = batch_id in self._stopped
                event = self._events.setdefault(batch_id, threading.Event())

            if stopped:
                yield "data: STOPPED\n\n"
                break

            if entry is not None:
                img, _, counter = entry
                if counter != last_counter:
                    last_counter = counter
                    b64 = base64.b64encode(img).decode()
                    yield f"data: {b64}\n\n"

            event.wait(timeout=1.0)
            event.clear()

_hub = PublishFrameHub()

def get_hub() -> PublishFrameHub:
    return _hub
