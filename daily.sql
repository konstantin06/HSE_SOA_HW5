SELECT
    count() AS event_count,
    count(DISTINCT user_id) AS dau,
    sumIf(progress_seconds, event_type = 'VIEW_FINISHED') * 1.0
        / nullIf(countIf(event_type = 'VIEW_FINISHED'), 0) AS avg_watch_seconds,
    countIf(event_type = 'VIEW_FINISHED') * 1.0
        / nullIf(countIf(event_type = 'VIEW_STARTED'), 0) AS conversion
FROM cinema.events
WHERE event_date = {day:Date}
