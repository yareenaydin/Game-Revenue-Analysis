WITH payments AS (
    SELECT 
        DATE_TRUNC('month', payment_date::timestamp) AS payment_month,
        user_id,
        SUM(revenue_amount_usd) AS total_revenue
    FROM games_payments
    GROUP BY payment_month, user_id
),
metrics AS (
    SELECT 
        p.payment_month,
        p.user_id,
        p.total_revenue,
        CASE 
            WHEN p.payment_month = MIN(p.payment_month) OVER (PARTITION BY p.user_id) 
            THEN p.total_revenue 
            ELSE NULL 
        END AS new_mrr,
        CASE 
            WHEN p.total_revenue > LAG(p.total_revenue) OVER (PARTITION BY p.user_id ORDER BY p.payment_month)
            THEN p.total_revenue - LAG(p.total_revenue) OVER (PARTITION BY p.user_id ORDER BY p.payment_month)
            ELSE NULL
        END AS expansion_revenue,
        CASE 
            WHEN p.total_revenue < LAG(p.total_revenue) OVER (PARTITION BY p.user_id ORDER BY p.payment_month)
            THEN p.total_revenue - LAG(p.total_revenue) OVER (PARTITION BY p.user_id ORDER BY p.payment_month)
            ELSE NULL
        END AS contraction_revenue,
        CASE 
            WHEN LAG(p.payment_month) OVER (PARTITION BY p.user_id ORDER BY p.payment_month) IS NOT NULL
                 AND (
                    (EXTRACT(YEAR FROM p.payment_month) * 12 + EXTRACT(MONTH FROM p.payment_month))
                    - (EXTRACT(YEAR FROM LAG(p.payment_month) OVER (PARTITION BY p.user_id ORDER BY p.payment_month)) * 12 
                       + EXTRACT(MONTH FROM LAG(p.payment_month) OVER (PARTITION BY p.user_id ORDER BY p.payment_month))
                    ) > 1
                 )
            THEN p.total_revenue
            ELSE NULL
        END AS back_from_churn_revenue,
        CASE 
            WHEN LEAD(p.payment_month) OVER (PARTITION BY p.user_id ORDER BY p.payment_month) IS NULL
                 OR (
                    (EXTRACT(YEAR FROM LEAD(p.payment_month) OVER (PARTITION BY p.user_id ORDER BY p.payment_month)) * 12 
                     + EXTRACT(MONTH FROM LEAD(p.payment_month) OVER (PARTITION BY p.user_id ORDER BY p.payment_month)))
                    - (EXTRACT(YEAR FROM p.payment_month) * 12 + EXTRACT(MONTH FROM p.payment_month))
                 ) > 1
            THEN p.total_revenue
            ELSE NULL
        END AS churned_revenue,
        CASE 
            WHEN LEAD(p.payment_month) OVER (PARTITION BY p.user_id ORDER BY p.payment_month) IS NULL
                 OR (
                    (EXTRACT(YEAR FROM LEAD(p.payment_month) OVER (PARTITION BY p.user_id ORDER BY p.payment_month)) * 12 
                     + EXTRACT(MONTH FROM LEAD(p.payment_month) OVER (PARTITION BY p.user_id ORDER BY p.payment_month)))
                    - (EXTRACT(YEAR FROM p.payment_month) * 12 + EXTRACT(MONTH FROM p.payment_month))
                 ) > 1
            THEN p.payment_month + INTERVAL '1 month'
            ELSE NULL
        END AS churn_month
    FROM payments p
)
SELECT 
    m.payment_month,
    m.user_id,
    m.total_revenue,
    m.new_mrr,
    m.expansion_revenue,
    m.contraction_revenue,
    m.back_from_churn_revenue,
    m.churned_revenue,
    m.churn_month,
    g.game_name,
    g.language,
    g.has_older_device_model,
    g.age
FROM metrics m
JOIN games_paid_users g 
    ON m.user_id = g.user_id
ORDER BY m.user_id, m.payment_month ASC;

