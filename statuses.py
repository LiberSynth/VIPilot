KNOWN_BATCH_STATUSES = frozenset({
    'pending', 'story_generating',
    'story_ready', 'video_generating', 'video_pending',
    'video_ready', 'transcoding',
    'transcode_ready',
    'story_probe',
    'cancelled', 'error', 'movie_probe', 'donated',
    'video_error', 'transcode_error', 'publish_error', 'published',
    'published_partially', 'fatal_error',
})

FINAL_BATCH_STATUSES = (
    'published', 'published_partially', 'movie_probe', 'story_probe',
    'cancelled', 'error', 'fatal_error',
    'video_error', 'transcode_error', 'publish_error',
    'donated',
)

PIPELINE_RESET_STATUS = {
    'story':     'pending',
    'video':     'story_ready',
    'transcode': 'video_ready',
    'publish':   'transcode_ready',
}

_COMPOSITE_STATUS_SUFFIXES = ('.posting', '.published', '.pending', '.failed')

PUBLISH_ROUTING_SUFFIXES = ('.pending', '.published')

_dynamic_statuses: set = set()


def register_dynamic_statuses(s: set) -> None:
    global _dynamic_statuses
    _dynamic_statuses = set(s)


def _assert_known_status(status: str) -> None:
    if status in KNOWN_BATCH_STATUSES:
        return
    if any(status.endswith(sfx) for sfx in _COMPOSITE_STATUS_SUFFIXES):
        if status in _dynamic_statuses:
            return
    raise ValueError(
        f"[DB] Неизвестный статус '{status}' — добавь его в KNOWN_BATCH_STATUSES или проверь конфиг таргета"
    )
