from datetime import UTC, date, datetime, time, timedelta, timezone
from uuid import NAMESPACE_URL, uuid5

DAY = date(2020, 4, 1)


def event(user, movie, kind, day=DAY, second=0, progress=0, session_id=None):
    instant = datetime.combine(day, time(10, 0, 0, 123000), UTC) + timedelta(seconds=second)
    stamp = instant.astimezone(timezone(timedelta(hours=3))).isoformat(timespec="milliseconds")
    session_id = session_id or f"{user}-{day}"
    return {"event_id": str(uuid5(NAMESPACE_URL, f"{session_id}/{kind}/{instant}")),
            "user_id": user, "movie_id": movie, "event_type": kind, "timestamp": stamp,
            "device_type": "TABLET", "session_id": session_id, "progress_seconds": progress}


def sample_events():
    return [
        event("anna", "a", "SEARCHED", DAY - timedelta(days=2)),
        event("old", "b", "VIEW_STARTED", DAY - timedelta(days=1)),
        event("anna", "a", "VIEW_STARTED"),
        event("anna", "a", "VIEW_PAUSED", second=120, progress=120),
        event("anna", "a", "VIEW_RESUMED", second=180, progress=121),
        event("anna", "a", "VIEW_FINISHED", second=1260, progress=1200),
        event("anna", "a", "LIKED", second=1261),
        event("boris", "b", "VIEW_STARTED"),
        event("boris", "b", "VIEW_FINISHED", second=600, progress=600),
        event("old", "a", "VIEW_STARTED"),
        event("guest", "z", "SEARCHED"),
        event("anna", "x", "SEARCHED", DAY + timedelta(days=1)),
        event("anna", "x", "LIKED", DAY + timedelta(days=1), second=5),
        event("old", "x", "SEARCHED", DAY + timedelta(days=1)),
        event("boris", "b", "LIKED", DAY + timedelta(days=2)),
        event("boris", "x", "SEARCHED", DAY + timedelta(days=7)),
        event("boris", "y", "SEARCHED", DAY + timedelta(days=7), second=8),
    ]
