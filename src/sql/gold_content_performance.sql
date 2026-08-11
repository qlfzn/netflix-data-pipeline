SELECT
    m.movie_id,
    m.title,
    SUM(wh.watch_duration_minutes) / 60 AS total_watch_hours,
    COUNT(DISTINCT wh.user_id) AS unique_viewers,
    AVG(r.rating) AS avg_rating,
    AVG(wh.progress_percentage) AS avg_completion
FROM movies m
LEFT JOIN watch_history wh ON m.movie_id = wh.movie_id
LEFT JOIN reviews r ON m.movie_id = r.movie_id
GROUP BY m.movie_id, m.title