import random

from db import cycle_config_get, db_get_graded_stories, db_get_used_stories
from utils.utils import wrap_block

def _get_video_duration() -> int:
    return max(1, min(60, cycle_config_get('video_duration')))

def _get_duration() -> str:
    return str(_get_video_duration())

def _get_word_count() -> str:
    return str(round(_get_video_duration() * cycle_config_get("words_per_second")))

def _get_good_samples() -> str:
    stories = db_get_graded_stories()
    good = [s for s in stories if s['grade'] == 'good']
    if good:
        limit = int(cycle_config_get('good_samples_count') or 25)
        good = random.sample(good, min(limit, len(good)))
    parts = [
        wrap_block('Образец хорошего качества', f'{story["title"]}\n\n{story["content"]}', i)
        for i, story in enumerate(good, start=1)
    ]
    return '\n\n'.join(parts)

def _get_bad_samples() -> str:
    stories = db_get_graded_stories()
    bad = [s for s in stories if s['grade'] == 'bad']
    parts = [
        wrap_block('Образец плохого качества', f'{story["title"]}\n\n{story["content"]}', i)
        for i, story in enumerate(bad, start=1)
    ]
    return '\n\n'.join(parts)

def _get_used_plots() -> str:
    stories = db_get_used_stories()
    parts = [
        wrap_block('Использованный сюжет', f'{story["title"]}\n\n{story["content"]}', i)
        for i, story in enumerate(stories, start=1)
    ]
    return '\n\n'.join(parts)

_PARAMS = [
    ('{продолжительность}', _get_duration),
    ('{количество_слов}', _get_word_count),
    ('{хорошие_образцы}', _get_good_samples),
    ('{плохие_образцы}', _get_bad_samples),
    ('{использованные_сюжеты}', _get_used_plots),
]

def apply_prompt_params(
    text: str,
    *,
    story_content: str | None = None,
    duration_seconds: int | None = None,
) -> str:
    if story_content is not None and '{сюжет}' in text:
        text = text.replace('{сюжет}', story_content)
    if duration_seconds is not None and '{продолжительность}' in text:
        text = text.replace('{продолжительность}', str(duration_seconds))
    for param, getter in _PARAMS:
        if param == '{продолжительность}' and duration_seconds is not None:
            continue
        if param in text:
            text = text.replace(param, getter())
    return text
