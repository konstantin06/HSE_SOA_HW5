SELECT movie_id, count() AS views
FROM cinema.events
WHERE event_date = {day:Date} AND event_type = 'VIEW_STARTED'
GROUP BY movie_id
ORDER BY views DESC, movie_id ASC
