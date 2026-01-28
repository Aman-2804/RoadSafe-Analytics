-- ============================================================================
-- RoadSafe Analytics - SQL Queries
-- ============================================================================
-- These queries demonstrate strong SQL skills for ICBC job requirements.
-- Run via Spark SQL or load Gold tables into a metastore.
-- ============================================================================

-- ============================================================================
-- 1. TOP HOTSPOTS (Location Analysis)
-- ============================================================================
-- Find locations with highest collision frequency
-- ============================================================================

SELECT 
    l.borough,
    l.zip_code,
    l.location_name,
    COUNT(fc.collision_id) as collision_count,
    SUM(fc.persons_injured) as total_injured,
    SUM(fc.persons_killed) as total_killed,
    AVG(fc.persons_injured + fc.persons_killed) as avg_persons_affected
FROM fact_collisions fc
JOIN dim_location l ON fc.location_key = l.location_key
GROUP BY l.borough, l.zip_code, l.location_name
ORDER BY collision_count DESC
LIMIT 20;

-- Alternative: Top hotspots by borough only
SELECT 
    l.borough,
    COUNT(fc.collision_id) as collision_count,
    SUM(fc.persons_injured) as total_injured,
    SUM(fc.persons_killed) as total_killed,
    ROUND(AVG(fc.persons_injured + fc.persons_killed), 2) as avg_persons_affected
FROM fact_collisions fc
JOIN dim_location l ON fc.location_key = l.location_key
WHERE l.borough IS NOT NULL
GROUP BY l.borough
ORDER BY collision_count DESC;

-- ============================================================================
-- 2. WORST HOURS (Time Distribution Analysis)
-- ============================================================================
-- Identify hours with highest collision frequency
-- ============================================================================

SELECT 
    t.hour,
    t.time_category,
    COUNT(fc.collision_id) as collision_count,
    SUM(fc.persons_injured) as total_injured,
    SUM(fc.persons_killed) as total_killed,
    ROUND(AVG(fc.persons_injured + fc.persons_killed), 2) as avg_severity
FROM fact_collisions fc
JOIN dim_time t ON fc.time_key = t.time_key
WHERE t.hour IS NOT NULL
GROUP BY t.hour, t.time_category
ORDER BY collision_count DESC;

-- Worst hours by day of week
SELECT 
    t.day_name,
    t.hour,
    COUNT(fc.collision_id) as collision_count,
    SUM(fc.persons_injured) as total_injured,
    SUM(fc.persons_killed) as total_killed
FROM fact_collisions fc
JOIN dim_time t ON fc.time_key = t.time_key
WHERE t.hour IS NOT NULL AND t.day_name IS NOT NULL
GROUP BY t.day_name, t.hour
ORDER BY collision_count DESC
LIMIT 20;

-- ============================================================================
-- 3. TREND OVER TIME (Monthly Analysis)
-- ============================================================================
-- Analyze collision trends by month/year
-- ============================================================================

SELECT 
    t.year,
    t.month,
    t.month_name,
    COUNT(fc.collision_id) as collision_count,
    SUM(fc.persons_injured) as total_injured,
    SUM(fc.persons_killed) as total_killed,
    ROUND(AVG(fc.persons_injured + fc.persons_killed), 2) as avg_severity
FROM fact_collisions fc
JOIN dim_time t ON fc.time_key = t.time_key
GROUP BY t.year, t.month, t.month_name
ORDER BY t.year DESC, t.month DESC;

-- Year-over-year comparison
SELECT 
    t.year,
    COUNT(fc.collision_id) as collision_count,
    SUM(fc.persons_injured) as total_injured,
    SUM(fc.persons_killed) as total_killed,
    ROUND(AVG(fc.persons_injured + fc.persons_killed), 2) as avg_severity
FROM fact_collisions fc
JOIN dim_time t ON fc.time_key = t.time_key
GROUP BY t.year
ORDER BY t.year DESC;

-- ============================================================================
-- 4. CLAIM AMOUNT BY BOROUGH (Financial Analysis)
-- ============================================================================
-- Analyze claim costs by geographic location
-- ============================================================================

SELECT 
    l.borough,
    COUNT(DISTINCT fc.collision_id) as collision_count,
    COUNT(claims.claim_id) as claim_count,
    ROUND(AVG(claims.claim_amount), 2) as avg_claim_amount,
    ROUND(SUM(claims.claim_amount), 2) as total_claim_amount,
    ROUND(MIN(claims.claim_amount), 2) as min_claim_amount,
    ROUND(MAX(claims.claim_amount), 2) as max_claim_amount
FROM fact_collisions fc
JOIN dim_location l ON fc.location_key = l.location_key
LEFT JOIN fact_claims claims ON fc.collision_id = claims.collision_id
WHERE l.borough IS NOT NULL
GROUP BY l.borough
ORDER BY total_claim_amount DESC;

-- Claim amount by severity and borough
SELECT 
    l.borough,
    fc.severity_category,
    COUNT(claims.claim_id) as claim_count,
    ROUND(AVG(claims.claim_amount), 2) as avg_claim_amount,
    ROUND(SUM(claims.claim_amount), 2) as total_claim_amount
FROM fact_collisions fc
JOIN dim_location l ON fc.location_key = l.location_key
JOIN fact_claims claims ON fc.collision_id = claims.collision_id
WHERE l.borough IS NOT NULL
GROUP BY l.borough, fc.severity_category
ORDER BY l.borough, total_claim_amount DESC;

-- ============================================================================
-- 5. CONTRIBUTING FACTORS RANKING
-- ============================================================================
-- Identify most common contributing factors
-- ============================================================================

-- Primary contributing factors
SELECT 
    f.contributing_factor,
    f.factor_category,
    COUNT(fc.collision_id) as collision_count,
    SUM(fc.persons_injured) as total_injured,
    SUM(fc.persons_killed) as total_killed,
    ROUND(AVG(fc.persons_injured + fc.persons_killed), 2) as avg_severity
FROM fact_collisions fc
JOIN dim_factor f ON fc.primary_factor_key = f.factor_key
GROUP BY f.contributing_factor, f.factor_category
ORDER BY collision_count DESC
LIMIT 20;

-- Contributing factors by category
SELECT 
    f.factor_category,
    COUNT(fc.collision_id) as collision_count,
    SUM(fc.persons_injured) as total_injured,
    SUM(fc.persons_killed) as total_killed,
    ROUND(AVG(fc.persons_injured + fc.persons_killed), 2) as avg_severity
FROM fact_collisions fc
JOIN dim_factor f ON fc.primary_factor_key = f.factor_key
GROUP BY f.factor_category
ORDER BY collision_count DESC;

-- ============================================================================
-- 6. VEHICLE TYPE ANALYSIS
-- ============================================================================
-- Analyze collisions by vehicle type
-- ============================================================================

SELECT 
    v.vehicle_type_name,
    COUNT(fc.collision_id) as collision_count,
    SUM(fc.persons_injured) as total_injured,
    SUM(fc.persons_killed) as total_killed,
    ROUND(AVG(fc.persons_injured + fc.persons_killed), 2) as avg_severity
FROM fact_collisions fc
JOIN dim_vehicle v ON fc.primary_vehicle_key = v.vehicle_key
GROUP BY v.vehicle_type_name
ORDER BY collision_count DESC;

-- Vehicle type with claim amounts
SELECT 
    v.vehicle_type_name,
    COUNT(DISTINCT fc.collision_id) as collision_count,
    COUNT(claims.claim_id) as claim_count,
    ROUND(AVG(claims.claim_amount), 2) as avg_claim_amount,
    ROUND(SUM(claims.claim_amount), 2) as total_claim_amount
FROM fact_collisions fc
JOIN dim_vehicle v ON fc.primary_vehicle_key = v.vehicle_key
LEFT JOIN fact_claims claims ON fc.collision_id = claims.collision_id
GROUP BY v.vehicle_type_name
ORDER BY total_claim_amount DESC;

-- ============================================================================
-- 7. SEVERITY ANALYSIS
-- ============================================================================
-- Analyze collision severity patterns
-- ============================================================================

SELECT 
    fc.severity_category,
    COUNT(fc.collision_id) as collision_count,
    SUM(fc.persons_injured) as total_injured,
    SUM(fc.persons_killed) as total_killed,
    ROUND(COUNT(fc.collision_id) * 100.0 / (SELECT COUNT(*) FROM fact_collisions), 2) as percentage
FROM fact_collisions fc
GROUP BY fc.severity_category
ORDER BY 
    CASE fc.severity_category
        WHEN 'Fatal' THEN 1
        WHEN 'Injury' THEN 2
        WHEN 'Property Damage Only' THEN 3
    END;

-- Severity by time of day
SELECT 
    t.time_category,
    fc.severity_category,
    COUNT(fc.collision_id) as collision_count
FROM fact_collisions fc
JOIN dim_time t ON fc.time_key = t.time_key
GROUP BY t.time_category, fc.severity_category
ORDER BY t.time_category, collision_count DESC;

-- ============================================================================
-- 8. CLAIMS ANALYSIS
-- ============================================================================
-- Analyze claim patterns and costs
-- ============================================================================

-- Claim status distribution
SELECT 
    claims.claim_status,
    COUNT(claims.claim_id) as claim_count,
    ROUND(AVG(claims.claim_amount), 2) as avg_claim_amount,
    ROUND(SUM(claims.claim_amount), 2) as total_claim_amount,
    ROUND(COUNT(claims.claim_id) * 100.0 / (SELECT COUNT(*) FROM fact_claims), 2) as percentage
FROM fact_claims claims
GROUP BY claims.claim_status;

-- Days to close analysis
SELECT 
    claims.claim_status,
    COUNT(claims.claim_id) as claim_count,
    ROUND(AVG(claims.days_to_close), 2) as avg_days_to_close,
    MIN(claims.days_to_close) as min_days,
    MAX(claims.days_to_close) as max_days
FROM fact_claims claims
WHERE claims.claim_status = 'CLOSED'
GROUP BY claims.claim_status;

-- Claim amount by severity
SELECT 
    fc.severity_category,
    COUNT(claims.claim_id) as claim_count,
    ROUND(AVG(claims.claim_amount), 2) as avg_claim_amount,
    ROUND(MIN(claims.claim_amount), 2) as min_claim_amount,
    ROUND(MAX(claims.claim_amount), 2) as max_claim_amount,
    ROUND(SUM(claims.claim_amount), 2) as total_claim_amount
FROM fact_claims claims
JOIN fact_collisions fc ON claims.collision_id = fc.collision_id
GROUP BY fc.severity_category
ORDER BY avg_claim_amount DESC;

-- ============================================================================
-- 9. WEEKEND VS WEEKDAY ANALYSIS
-- ============================================================================
-- Compare collision patterns on weekends vs weekdays
-- ============================================================================

SELECT 
    t.is_weekend,
    COUNT(fc.collision_id) as collision_count,
    SUM(fc.persons_injured) as total_injured,
    SUM(fc.persons_killed) as total_killed,
    ROUND(AVG(fc.persons_injured + fc.persons_killed), 2) as avg_severity
FROM fact_collisions fc
JOIN dim_time t ON fc.time_key = t.time_key
GROUP BY t.is_weekend
ORDER BY t.is_weekend;

-- ============================================================================
-- 10. COMPREHENSIVE DASHBOARD QUERY
-- ============================================================================
-- Summary statistics for dashboard
-- ============================================================================

SELECT 
    'Total Collisions' as metric,
    CAST(COUNT(DISTINCT fc.collision_id) AS STRING) as value
FROM fact_collisions fc
UNION ALL
SELECT 
    'Total Claims' as metric,
    CAST(COUNT(DISTINCT claims.claim_id) AS STRING) as value
FROM fact_claims claims
UNION ALL
SELECT 
    'Total Claim Amount' as metric,
    CAST(ROUND(SUM(claims.claim_amount), 2) AS STRING) as value
FROM fact_claims claims
UNION ALL
SELECT 
    'Avg Claim Amount' as metric,
    CAST(ROUND(AVG(claims.claim_amount), 2) AS STRING) as value
FROM fact_claims claims
UNION ALL
SELECT 
    'Total Injuries' as metric,
    CAST(SUM(fc.persons_injured) AS STRING) as value
FROM fact_collisions fc
UNION ALL
SELECT 
    'Total Fatalities' as metric,
    CAST(SUM(fc.persons_killed) AS STRING) as value
FROM fact_collisions fc;




