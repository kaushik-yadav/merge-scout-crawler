from datetime import datetime


def parse_time(value: str | None) -> datetime | None:
    # GitHub times look like 2026-10-01T15:00:01Z
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
