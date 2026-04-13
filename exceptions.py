"""
Единый класс исключения для прерывающих ошибок в пайплайнах.
"""


class FatalError(Exception):
    """Критическое нарушение инварианта приложения вне контекста пайплайна.

    Бросается когда ситуация невозможна/недопустима на уровне всего приложения,
    но не привязана к конкретному батчу или пайплайну.
    Пример: неизвестный статус батча в statuses.py.

    Атрибуты:
        message — человекочитаемое описание ошибки
    """

    def __init__(self, message: str):
        super().__init__(message)
        self.message = message

    def __str__(self):
        return self.message


class AppException(Exception):
    """Прерывающая ошибка пайплайна.

    Бросается вместо паттерна «if ... log; return» при ситуациях,
    которые делают дальнейшую обработку батча невозможной.
    Статус батча устанавливается централизованно в _batch_thread.

    Атрибуты:
        batch_id  — идентификатор батча (может быть None для системных ошибок)
        pipeline  — имя пайплайна ('story', 'video', 'transcode', 'publish', ...)
        message   — человекочитаемое описание ошибки
        log_id    — идентификатор записи лога (из db_log_pipeline), созданной до броска
    """

    def __init__(self, batch_id, pipeline: str, message: str, log_id=None):
        super().__init__(message)
        self.batch_id = batch_id
        self.pipeline = pipeline
        self.message = message
        self.log_id = log_id

    def __str__(self):
        return self.message
