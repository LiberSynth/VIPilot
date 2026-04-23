from db import cycle_config_get, db_get_graded_stories


def _get_video_duration() -> int:
    return max(1, min(60, cycle_config_get()['video_duration']))


def _get_duration() -> str:
    return str(_get_video_duration())


def _get_word_count() -> str:
    return str(round(_get_video_duration() * cycle_config_get()["words_per_second"]))


def _get_sample_list() -> str:
    stories = db_get_graded_stories()
    if not stories:
        return ''
    good = [s for s in stories if s['grade'] == 'good']
    bad  = [s for s in stories if s['grade'] == 'bad']
    parts = []
    for i, story in enumerate(good, start=1):
        parts.append(
            f'/* Образец хорошего качества {i} НАЧАЛО */\n\n'
            f'{story["title"]}\n\n'
            f'{story["content"]}\n\n'
            f'/* Образец хорошего качества {i} КОНЕЦ */'
        )
    for i, story in enumerate(bad, start=1):
        parts.append(
            f'/* Образец плохого качества {i} НАЧАЛО */\n\n'
            f'{story["title"]}\n\n'
            f'{story["content"]}\n\n'
            f'/* Образец плохого качества {i} КОНЕЦ */'
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
