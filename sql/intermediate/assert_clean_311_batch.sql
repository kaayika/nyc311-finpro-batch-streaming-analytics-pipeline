-- ========================================
-- INTERMEDIATE DATA QUALITY ASSERTIONS
-- ========================================

-- 1. ROW COUNT MATCHES STAGING
ASSERT (
  SELECT
    (
      SELECT COUNT(*)
      FROM `jcdeah-009.nyc311_finpro_hita_intermediate.clean_311_batch`
    )
    =
    (
      SELECT COUNT(*)
      FROM `jcdeah-009.nyc311_finpro_hita_staging.raw_311_batch`
    )
)
AS 'Intermediate row count does not match staging';


-- 2. DUPLICATE UNIQUE KEY
ASSERT (
  SELECT
    COUNT(*) - COUNT(DISTINCT unique_key)
  FROM `jcdeah-009.nyc311_finpro_hita_intermediate.clean_311_batch`
) = 0
AS 'Duplicate unique_key found in intermediate';


-- 3. CRITICAL NULL
ASSERT (
  SELECT
    COUNTIF(
      unique_key IS NULL
      OR created_date IS NULL
      OR agency IS NULL
      OR complaint_type IS NULL
      OR status IS NULL
      OR borough IS NULL
    )
  FROM `jcdeah-009.nyc311_finpro_hita_intermediate.clean_311_batch`
) = 0
AS 'Critical NULL value found in intermediate';


-- 4. DATE RANGE
ASSERT (
  SELECT
    MIN(created_date) >= TIMESTAMP('2026-01-01')
    AND
    MAX(created_date) < TIMESTAMP('2026-04-01')
  FROM `jcdeah-009.nyc311_finpro_hita_intermediate.clean_311_batch`
)
AS 'Invalid intermediate created_date range';


-- 5. INVALID COORDINATE
ASSERT (
  SELECT
    COUNTIF(
      (latitude IS NOT NULL AND NOT latitude BETWEEN -90 AND 90)
      OR
      (longitude IS NOT NULL AND NOT longitude BETWEEN -180 AND 180)
    )
  FROM `jcdeah-009.nyc311_finpro_hita_intermediate.clean_311_batch`
) = 0
AS 'Invalid coordinate found in intermediate';


-- 6. NEGATIVE RESOLUTION TIME
ASSERT (
  SELECT
    COUNTIF(resolution_time_hours < 0)
  FROM `jcdeah-009.nyc311_finpro_hita_intermediate.clean_311_batch`
) = 0
AS 'Negative resolution time found in intermediate';
