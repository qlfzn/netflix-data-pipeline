SELECT
	DATE_FORMAT(CAST(subscription_start_date AS DATE), 'yyyy-MM') AS subscription_month,
	subscription_plan,
	COUNT(*) AS new_subscribers,
	SUM(monthly_spend) AS monthly_revenue,
	AVG(monthly_spend) AS average_monthly_spend,
	SUM(CASE WHEN is_active = TRUE THEN 1 ELSE 0 END) AS active_subscribers,
	SUM(CASE WHEN is_active = FALSE THEN 1 ELSE 0 END) AS inactive_subscribers
FROM users
GROUP BY DATE_FORMAT(CAST(subscription_start_date AS DATE), 'yyyy-MM'), subscription_plan
