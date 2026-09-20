-- ========================================
-- STAGING DATA QUALITY ASSERTIONS
-- ========================================

-- 1. ROW COUNT
ASSERT (
  SELECT COUNT(*)
  FROM `jcdeah-009.nyc311_finpro_hita_staging.raw_311_batch`
) = 1025589
AS 'Staging row count mismatch';


-- 2. DUPLICATE UNIQUE KEY
ASSERT (
  SELECT COUNT(*) - COUNT(DISTINCT unique_key)
  FROM `jcdeah-009.nyc311_finpro_hita_staging.raw_311_batch`
) = 0
AS 'Duplicate unique_key found in staging';


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
  FROM `jcdeah-009.nyc311_finpro_hita_staging.raw_311_batch`
) = 0
AS 'Critical NULL value found in staging';


-- 4. DATE RANGE
ASSERT (
  SELECT
    MIN(created_date) >= TIMESTAMP('2026-01-01')
    AND
    MAX(created_date) < TIMESTAMP('2026-04-01')
  FROM `jcdeah-009.nyc311_finpro_hita_staging.raw_311_batch`
)
AS 'Invalid staging created_date range';
