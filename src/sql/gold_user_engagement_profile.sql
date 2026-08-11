WITH watch_stats AS (
	SELECT
		user_id,
		COUNT(*) AS watch_sessions,
		COUNT(DISTINCT movie_id) AS unique_titles_watched,
		SUM(watch_duration_minutes) AS total_watch_minutes,
		AVG(progress_percentage) AS average_completion,
		AVG(user_rating) AS average_user_rating
	FROM watch_history
	GROUP BY user_id
),
search_stats AS (
	SELECT
		user_id,
		COUNT(*) AS total_searches,
		AVG(search_duration_seconds) AS average_search_duration,
		SUM(CASE WHEN used_filters = TRUE THEN 1 ELSE 0 END) AS searches_with_filters
	FROM search_logs
	GROUP BY user_id
),
recommendation_stats AS (
	SELECT
		user_id,
		COUNT(*) AS recommendation_impressions,
		SUM(CASE WHEN was_clicked = TRUE THEN 1 ELSE 0 END) AS clicked_recommendations,
		AVG(recommendation_score) AS average_recommendation_score
	FROM recommendation_logs
	GROUP BY user_id
),
review_stats AS (
	SELECT
		user_id,
		COUNT(*) AS total_reviews,
		AVG(rating) AS average_rating_given,
		SUM(helpful_votes) AS total_helpful_votes
	FROM reviews
	GROUP BY user_id
)
SELECT
	u.user_id,
	u.country,
	u.subscription_plan,
	u.primary_device,
	COALESCE(w.watch_sessions, 0) AS watch_sessions,
	COALESCE(w.unique_titles_watched, 0) AS unique_titles_watched,
	COALESCE(w.total_watch_minutes, 0) AS total_watch_minutes,
	COALESCE(w.average_completion, 0) AS average_completion,
	COALESCE(w.average_user_rating, 0) AS average_user_rating,
	COALESCE(s.total_searches, 0) AS total_searches,
	COALESCE(s.average_search_duration, 0) AS average_search_duration,
	COALESCE(s.searches_with_filters, 0) AS searches_with_filters,
	COALESCE(r.recommendation_impressions, 0) AS recommendation_impressions,
	COALESCE(r.clicked_recommendations, 0) AS clicked_recommendations,
	COALESCE(r.average_recommendation_score, 0) AS average_recommendation_score,
	COALESCE(v.total_reviews, 0) AS total_reviews,
	COALESCE(v.average_rating_given, 0) AS average_rating_given,
	COALESCE(v.total_helpful_votes, 0) AS total_helpful_votes
FROM users u
LEFT JOIN watch_stats w ON u.user_id = w.user_id
LEFT JOIN search_stats s ON u.user_id = s.user_id
LEFT JOIN recommendation_stats r ON u.user_id = r.user_id
LEFT JOIN review_stats v ON u.user_id = v.user_id
