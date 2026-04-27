def wrap_block(title: str, body: str, number: int | None = None) -> str:
    """Оборачивает тело в блок с маркерами НАЧАЛО/КОНЕЦ."""
    label = f'{title} {number}' if number is not None else title
    return f'/* {label} НАЧАЛО */\n{body}\n/* {label} КОНЕЦ */'


def fmt_id_msg(template: str, *ids) -> str:
    """Подставляет каждый идентификатор целиком (без обрезки) в соответствующий {} плейсхолдер шаблона."""
    return template.format(*[str(i) for i in ids])


def parse_hhmm(s):
    try:
        h, m = s.strip().split(":")
        return int(h) % 24, int(m) % 60
    except Exception:
        return 6, 0


def parse_batch_lifetime(s):
    try:
        v = int(s)
        return 0 if v == 0 else max(1, min(365, v))
    except Exception:
        return 7


def parse_long_lifetime(s, default=365):
    try:
        v = int(s)
        return 0 if v == 0 else max(1, min(3650, v))
    except Exception:
        return default


def parse_file_lifetime(s):
    try:
        v = int(s)
        return 0 if v == 0 else max(1, min(365, v))
    except Exception:
        return 7


def to_msk(h, m):
    total = (h * 60 + m + 180) % 1440
    return total // 60, total % 60


def to_utc_from_msk(h, m):
    total = (h * 60 + m - 180) % 1440
    return total // 60, total % 60


def nearest_allowed_duration(value: int, allowed: list[int]) -> int:
    """
    Возвращает ближайшее значение из allowed к value.
    Если allowed == [0] — возвращает value без изменений.
    При равноудалённости двух значений — берёт меньшее.
    """
    if allowed == [0]:
        return value
    best = None
    best_diff = None
    for a in allowed:
        diff = abs(a - value)
        if best_diff is None or diff < best_diff or (diff == best_diff and a < best):
            best = a
            best_diff = diff
    return best
