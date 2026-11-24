USE autopulse_db;

-- 1. Executive Fleet KPIs
SELECT
    COUNT(*) AS total_trips,
    ROUND(SUM(distance_km), 2) AS total_distance_km,
    ROUND(SUM(amount_inr), 2) AS total_spend_inr,
    ROUND(SUM(amount_inr) / NULLIF(SUM(distance_km), 0), 2) AS fleet_avg_cost_per_km,
    SUM(CASE WHEN is_outlier = 1 THEN 1 ELSE 0 END) AS total_outliers,
    ROUND(100.0 * SUM(CASE WHEN is_outlier = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS outlier_rate_pct
FROM trip_logs;

-- 2. Monthly Spend Trend & Month-over-Month (MoM) Growth %
WITH monthly_spend AS (
    SELECT
        DATE_FORMAT(trip_date, '%Y-%m') AS trip_month,
        COUNT(*) AS trips,
        ROUND(SUM(distance_km), 2) AS distance_km,
        ROUND(SUM(amount_inr), 2) AS spend_inr
    FROM trip_logs
    GROUP BY DATE_FORMAT(trip_date, '%Y-%m')
)
SELECT
    trip_month,
    trips,
    distance_km,
    spend_inr,
    LAG(spend_inr) OVER (ORDER BY trip_month) AS prev_month_spend,
    ROUND(
        100.0 * (spend_inr - LAG(spend_inr) OVER (ORDER BY trip_month))
        / NULLIF(LAG(spend_inr) OVER (ORDER BY trip_month), 0),
        2
    ) AS mom_growth_pct,
    ROUND(SUM(spend_inr) OVER (ORDER BY trip_month), 2) AS cumulative_spend_inr
FROM monthly_spend
ORDER BY trip_month;

-- 3. Vehicle Efficiency & Cost-Per-KM Ranking
WITH vehicle_metrics AS (
    SELECT
        v.vehicle_id,
        v.model_name,
        v.fuel_type,
        COUNT(t.vehicle_id) AS total_trips,
        ROUND(SUM(t.distance_km), 2) AS total_distance_km,
        ROUND(SUM(t.amount_inr), 2) AS total_spend_inr,
        ROUND(SUM(t.amount_inr) / NULLIF(SUM(t.distance_km), 0), 2) AS cost_per_km,
        SUM(CASE WHEN t.is_outlier = 1 THEN 1 ELSE 0 END) AS outlier_count
    FROM vehicles v
    JOIN trip_logs t ON v.vehicle_id = t.vehicle_id
    GROUP BY v.vehicle_id, v.model_name, v.fuel_type
)
SELECT
    RANK() OVER (ORDER BY cost_per_km DESC) AS inefficiency_rank,
    vehicle_id,
    model_name,
    fuel_type,
    total_trips,
    total_distance_km,
    total_spend_inr,
    cost_per_km,
    outlier_count
FROM vehicle_metrics
ORDER BY inefficiency_rank;

-- 4. Expense Category Breakdown & Share of Total Spend
SELECT
    txn_type,
    COUNT(*) AS txn_count,
    ROUND(SUM(amount_inr), 2) AS category_spend_inr,
    ROUND(AVG(amount_inr), 2) AS avg_txn_inr,
    ROUND(100.0 * SUM(amount_inr) / SUM(SUM(amount_inr)) OVER (), 2) AS spend_share_pct
FROM trip_logs
GROUP BY txn_type
ORDER BY category_spend_inr DESC;

-- 5. BI-Ready Analytical View
CREATE OR REPLACE VIEW vw_fleet_bi_summary AS
SELECT
    DATE(t.trip_date) AS trip_day,
    DATE_FORMAT(t.trip_date, '%Y-%m') AS trip_month,
    v.vehicle_id,
    v.model_name,
    v.fuel_type,
    t.txn_type,
    t.distance_km,
    t.amount_inr,
    t.is_outlier
FROM trip_logs t
JOIN vehicles v ON t.vehicle_id = v.vehicle_id;