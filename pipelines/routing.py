from pipelines import planning, story, prompt, video, transcode, publish
from common.statuses import PUBLISH_ROUTING_SUFFIXES

_TYPE_STATUS_TO_PIPELINE = {
    ('planning', 'pending'): planning,
    ('story', 'pending'):    story,
    ('prompt', 'pending'):   prompt,
    ('prompt', 'processing'): prompt,
    ('movie', 'pending'):    video,
    ('movie', 'processing'): video,
    ('movie', 'processed'):  video,
    ('transcode', 'pending'): transcode,
    ('transcode', 'processing'): transcode,
    ('publish', 'pending'): publish,
}

def get_pipeline(batch_type: str, status: str):
    """Возвращает модуль пайплайна для пары (type, status)."""
    pipeline = _TYPE_STATUS_TO_PIPELINE.get((batch_type, status))
    if pipeline is not None:
        return pipeline

    # Общие pending/processing обрабатываются только stage-маршрутизацией.
    if status in ('pending', 'processing'):
        return None

    if any(status.endswith(sfx) for sfx in PUBLISH_ROUTING_SUFFIXES):
        return publish
    return None
