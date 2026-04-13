"""
Единый класс исключения для прерывающих ошибок в пайплайнах.
"""


class AppException(Exception):
    """Прерывающая ошибка пайплайна.

    Бросается вместо паттерна «if ... log; return» при ситуациях,
    которые делают дальнейшую обработку батча невозможной.
    Статус батча устанавливается централизованно в _batch_thread.

    Атрибуты:
        batch_id  — идентификатор батча (может быть None для системных ошибок)
        pipeline  — имя пайплайна ('story', 'video', 'transcode', 'publish', ...)
        message   — человекочитаемое описание ошибки
    """

    def __init__(self, batch_id, pipeline: str, message: str):
        super().__init__(message)
        self.batch_id = batch_id
        self.pipeline = pipeline
        self.message = message

    def __str__(self):
        return self.message
