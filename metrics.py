from datetime import timedelta

from app.common import ROOT

SQL_DIR = ROOT / "sql/metrics"
DAILY_SQL = (SQL_DIR / "daily.sql").read_text()
MOVIES_SQL = (SQL_DIR / "movies.sql").read_text()
COHORT_SQL = (SQL_DIR / "cohort.sql").read_text()


def calculate(day, computed_at, read):
    daily, = read(DAILY_SQL, day)
    movies = read(MOVIES_SQL, day)
    ages = {row["age"]: row["users"] for row in read(COHORT_SQL, day)}
    size = ages.get(0, 0)
    retention = []
    for age in range(8):
        if size and day + timedelta(days=age) <= computed_at.date():
            retention.append(ages.get(age, 0) / size)
        else:
            retention.append(None)
    return {
        "metric_date": day.isoformat(),
        "computed_at": computed_at.strftime("%Y-%m-%d %H:%M:%S.%f"),
        **daily,
        "cohort_size": size,
        "retention": retention,
        "movie_ids": [row["movie_id"] for row in movies],
        "movie_views": [row["views"] for row in movies],
    }


def metric_values(snapshot):
    values = {(name, ""): snapshot[name] for name in (
        "event_count", "dau", "avg_watch_seconds", "conversion", "cohort_size"
    )}
    for age, rate in enumerate(snapshot["retention"]):
        values["cohort_retention", str(age)] = rate
    values["retention_d1", ""] = snapshot["retention"][1]
    values["retention_d7", ""] = snapshot["retention"][7]
    for movie, count in zip(snapshot["movie_ids"], snapshot["movie_views"], strict=True):
        values["movie_views", movie] = count
    return values
