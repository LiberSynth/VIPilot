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


def parse_log_lifetime(s):
    try:
        v = int(s)
        return 0 if v == 0 else max(1, min(3650, v))
    except Exception:
        return 365


def parse_entries_lifetime(s):
    try:
        v = int(s)
        return 0 if v == 0 else max(1, min(3650, v))
    except Exception:
        return 30


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
