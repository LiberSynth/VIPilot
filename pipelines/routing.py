from pipelines import story, video, transcode, publish
from common.statuses import PUBLISH_ROUTING_SUFFIXES

_TYPE_STATUS_TO_PIPELINE = {
    ('story', 'pending'):    story,
    ('story', 'generating'): story,
    ('movie', 'pending'):    video,
    ('movie', 'generating'): video,
}

_LEGACY_STATUS_TO_PIPELINE = {
    'video_generating': video,
    'video_pending':    video,
    'video_ready':      transcode,
    'transcoding':      transcode,
    'transcode_ready':  publish,
}

def get_pipeline(batch_type: str, status: str):
    """Возвращает модуль пайплайна для пары (type, status)."""
    pipeline = _TYPE_STATUS_TO_PIPELINE.get((batch_type, status))
    if pipeline is not None:
        return pipeline

    # Общие pending/generating обрабатываются только stage-маршрутизацией.
    if status in ('pending', 'generating'):
        return None

    pipeline = _LEGACY_STATUS_TO_PIPELINE.get(status)
    if pipeline is not None:
        return pipeline

    if any(status.endswith(sfx) for sfx in PUBLISH_ROUTING_SUFFIXES):
        return publish
    return None
