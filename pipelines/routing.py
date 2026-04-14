from pipelines import story, video, transcode, publish
from common.statuses import PUBLISH_ROUTING_SUFFIXES

_STATUS_TO_PIPELINE = {
    'pending':          story,
    'story_generating': story,
    'story_ready':      video,
    'video_generating': video,
    'video_pending':    video,
    'video_ready':      transcode,
    'transcoding':      transcode,
    'transcode_ready':  publish,
}


def get_pipeline(status: str):
    """Возвращает модуль пайплайна для данного статуса батча.
    Составные статусы вида slug.method.pending / slug.method.published → publish."""
    pipeline = _STATUS_TO_PIPELINE.get(status)
    if pipeline is not None:
        return pipeline
    if any(status.endswith(sfx) for sfx in PUBLISH_ROUTING_SUFFIXES):
        return publish
    return None
