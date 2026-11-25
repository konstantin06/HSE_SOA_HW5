WITH cohort AS
(
    SELECT user_id
    FROM cinema.events
    WHERE event_type = 'VIEW_STARTED' AND event_date <= {day:Date}
    GROUP BY user_id
    HAVING min(event_date) = {day:Date}
)
SELECT dateDiff('day', {day:Date}, event_date) AS age, uniqExact(user_id) AS users
FROM cinema.events
WHERE event_date BETWEEN {day:Date} AND addDays({day:Date}, 7)
  AND user_id IN (SELECT user_id FROM cohort)
GROUP BY age
ORDER BY age
