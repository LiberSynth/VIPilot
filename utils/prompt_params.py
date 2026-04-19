from db import db_get, db_get_top_quality_stories


def _get_video_duration() -> int:
    try:
        return max(1, min(60, int(db_get('video_duration', '6'))))
    except (ValueError, TypeError):
        return 6


def _get_duration() -> str:
    return str(_get_video_duration())


def _get_word_count() -> str:
    return str(_get_video_duration() * 4)


def _get_sample_list() -> str:
    stories = db_get_top_quality_stories()
    if not stories:
        return ''
    parts = []
    for i, story in enumerate(stories, start=1):
        parts.append(
            f'/* Образец {i} НАЧАЛО */\n\n'
            f'{story["title"]}\n\n'
            f'{story["content"]}\n\n'
            f'/* Образец {i} КОНЕЦ */'
        )
    return '\n\n'.join(parts)


_PARAMS = [
    ('{продолжительность}', _get_duration),
    ('{количество_слов}', _get_word_count),
    ('{список_образцов}', _get_sample_list),
]


def apply_prompt_params(text: str) -> str:
    for param, getter in _PARAMS:
        if param in text:
            text = text.replace(param, getter())
    return text
