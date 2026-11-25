import heapq
import os
import random
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from uuid import NAMESPACE_URL, uuid4, uuid5

from app.common import log
from app.events import MovieEvent

DEVICES = ("MOBILE", "DESKTOP", "TV", "TABLET")
HISTORY_DAYS = 12


def session(user, start, session_id, rng):
    movie = f"movie-{rng.randint(1, 12):02}"
    device = rng.choice(DEVICES)
    length = rng.randint(1200, 6600)
    steps = []
    if rng.random() < 0.55:
        steps.append(("SEARCHED", 0, 0))
    steps.append(("VIEW_STARTED", 10, 0))
    break_time = 0
    if rng.random() < 0.7:
        progress = rng.randint(60, length - 60)
        steps.append(("VIEW_PAUSED", 10 + progress, progress))
        if rng.random() < 0.85:
            break_time = rng.randint(15, 150)
            steps.append(("VIEW_RESUMED", 11 + progress + break_time, progress + 1))
    if steps[-1][0] != "VIEW_PAUSED" and rng.random() < 0.85:
        steps.append(("VIEW_FINISHED", 10 + length + break_time, length))
        if rng.random() < 0.4:
            steps.append(("LIKED", 15 + length + break_time, 0))
    for kind, offset, progress in steps:
        yield MovieEvent(event_id=uuid5(NAMESPACE_URL, f"{session_id}/{kind}"),
                        user_id=user, movie_id=movie, event_type=kind, device_type=device,
                        timestamp=start + timedelta(seconds=offset), session_id=session_id,
                        progress_seconds=progress)


def history(anchor):
    for days_ago in range(HISTORY_DAYS, 0, -1):
        cohort = anchor - timedelta(days=days_ago)
        for number in range(6):
            user = f"demo-{cohort}-{number}"
            for age in range(min(8, days_ago)):
                session_id = f"{user}/day-{age}"
                rng = random.Random(session_id)
                if age and rng.random() > 0.72 - 0.065 * age:
                    continue
                start = datetime.combine(cohort + timedelta(days=age), time(rng.randint(8, 20)), UTC)
                yield from session(user, start, session_id, rng)


def publish_all(events, send, stop):
    for event in events:
        wait_seconds = 1
        while not stop.is_set():
            try:
                send(event)
                break
            except Exception:
                log.exception("generator publish failed event_id=%s", event.event_id)
                stop.wait(wait_seconds)
                wait_seconds = min(wait_seconds * 2, 30)
        if stop.is_set():
            return False
    return True


def generate(send, stop):
    data_dir = Path(os.getenv("GENERATOR_DIR", "/var/lib/movie-generator"))
    data_dir.mkdir(parents=True, exist_ok=True)
    anchor_file = data_dir / "anchor"
    if not anchor_file.exists():
        temp = data_dir / "anchor.tmp"
        temp.write_text(datetime.now(UTC).date().isoformat())
        temp.replace(anchor_file)
    anchor = date.fromisoformat(anchor_file.read_text())
    marker = data_dir / "history-done"
    if not marker.exists():
        if not publish_all(history(anchor), send, stop):
            return
        marker.touch()
    rng = random.Random()
    interval = timedelta(seconds=max(0.1, float(os.getenv("GENERATOR_INTERVAL_SECONDS", "3"))))
    next_start = datetime.now(UTC)
    free_after = {f"live-{i:02}": next_start for i in range(1, 61)}
    pending = []
    while not stop.is_set():
        now = datetime.now(UTC)
        if now >= next_start:
            free_users = [user for user, end in free_after.items() if end <= now]
            if free_users:
                user = rng.choice(free_users)
                items = list(session(user, now, str(uuid4()), rng))
                free_after[user] = items[-1].timestamp
                for item in items:
                    heapq.heappush(pending, (item.timestamp, item.event_id, item))
            next_start = now + interval
        while pending and pending[0][0] <= now:
            _, _, item = heapq.heappop(pending)
            if not publish_all((item,), send, stop):
                return
        wake_at = min(next_start, pending[0][0]) if pending else next_start
        stop.wait(max(0.01, (wake_at - datetime.now(UTC)).total_seconds()))
