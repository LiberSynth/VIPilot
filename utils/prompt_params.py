from db import cycle_config_get, db_get_graded_stories, db_get_used_stories


def _get_video_duration() -> int:
    return max(1, min(60, cycle_config_get('video_duration')))


def _get_duration() -> str:
    return str(_get_video_duration())


def _get_word_count() -> str:
    return str(round(_get_video_duration() * cycle_config_get("words_per_second")))


def _get_good_samples() -> str:
    stories = db_get_graded_stories()
    good = [s for s in stories if s['grade'] == 'good']
    parts = [
        f'/* Образец хорошего качества {i} НАЧАЛО */\n\n'
        f'{story["title"]}\n\n'
        f'{story["content"]}\n\n'
        f'/* Образец хорошего качества {i} КОНЕЦ */'
        for i, story in enumerate(good, start=1)
    ]
    return '\n\n'.join(parts)


def _get_bad_samples() -> str:
    stories = db_get_graded_stories()
    bad = [s for s in stories if s['grade'] == 'bad']
    parts = [
        f'/* Образец плохого качества {i} НАЧАЛО */\n\n'
        f'{story["title"]}\n\n'
        f'{story["content"]}\n\n'
        f'/* Образец плохого качества {i} КОНЕЦ */'
        for i, story in enumerate(bad, start=1)
    ]
    return '\n\n'.join(parts)


def _get_used_plots() -> str:
    approve_movies = cycle_config_get('approve_movies')
    stories = db_get_used_stories(approve_movies)
    parts = [
        f'/* Использованный сюжет {i} НАЧАЛО */\n\n'
        f'{story["title"]}\n\n'
        f'{story["content"]}\n\n'
        f'/* Использованный сюжет {i} КОНЕЦ */'
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


def apply_prompt_params(text: str) -> str:
    for param, getter in _PARAMS:
        if param in text:
            text = text.replace(param, getter())
    return text
