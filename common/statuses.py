KNOWN_BATCH_STATUSES = frozenset({
    'pending', 'processing', 'generating', 'generated', 'ready',
    'video_generating', 'video_pending',
    'video_ready', 'transcoding',
    'cancelled', 'error', 'donated', 'reserved',
    'video_error', 'transcode_error', 'publish_error', 'published',
    'published_partially', 'fatal_error',
})

FINAL_BATCH_STATUSES = (
    'published', 'published_partially', 'ready',
    'cancelled', 'error', 'fatal_error',
    'video_error', 'transcode_error', 'publish_error',
    'donated', 'reserved',
)

PIPELINE_RESET_STATUS = {
    'story':     'pending',
    'video':     'pending',
    'transcode': 'pending',
    'publish':   'pending',
}

COMPOSITE_BATCH_STATUS_SUFFIXES = frozenset({'.posting', '.published', '.pending', '.failed'})

PUBLISH_ROUTING_SUFFIXES = ('.pending', '.published')

def batch_is_active(status: str) -> bool:
    """Возвращает True если батч находится в активном (не финальном) статусе."""
    return status not in FINAL_BATCH_STATUSES

def _assert_known_status(status: str) -> None:
    if status is None:
        return
    if status in KNOWN_BATCH_STATUSES:
        return
    # Для составных статусов вида {slug}.{method}.{phase} проверяется только суффикс (фаза).
    # {slug} и {method} не верифицируются — они приходят из статической конфигурации таргета
    # и проверяются на этапе его внедрения.
    if any(status.endswith(sfx) for sfx in COMPOSITE_BATCH_STATUS_SUFFIXES):
        return
    from common.exceptions import FatalError
    raise FatalError(
        f"[DB] Неизвестный статус '{status}' — добавь его в KNOWN_BATCH_STATUSES или проверь конфиг таргета"
    )
