-- ========================================
-- 1. ROW COUNT CONSISTENCY
-- ========================================

SELECT
  (SELECT COUNT(*)
   FROM `jcdeah-009.nyc311_finpro_hita_staging.raw_311_streaming`)
    AS staging_rows,

  (SELECT COUNT(*)
   FROM `jcdeah-009.nyc311_finpro_hita_intermediate.clean_311_streaming`)
    AS intermediate_rows;


-- ========================================
-- 2. DUPLICATE UNIQUE KEY
-- ========================================

SELECT
  COUNT(*) - COUNT(DISTINCT unique_key) AS duplicate_rows
FROM `jcdeah-009.nyc311_finpro_hita_intermediate.clean_311_streaming`;


-- ========================================
-- 3. CRITICAL NULL
-- ========================================

SELECT
  COUNTIF(unique_key IS NULL) AS null_unique_key,
  COUNTIF(created_date IS NULL) AS null_created_date,
  COUNTIF(agency IS NULL) AS null_agency,
  COUNTIF(complaint_type IS NULL) AS null_complaint_type,
  COUNTIF(status IS NULL) AS null_status,
  COUNTIF(borough IS NULL) AS null_borough
FROM `jcdeah-009.nyc311_finpro_hita_intermediate.clean_311_streaming`;


-- ========================================
-- 4. DATE RANGE
-- ========================================

SELECT
  MIN(created_date) AS min_created_date,
  MAX(created_date) AS max_created_date
FROM `jcdeah-009.nyc311_finpro_hita_intermediate.clean_311_streaming`;


-- ========================================
-- 5. INVALID COORDINATE
-- ========================================

SELECT
  COUNTIF(
    latitude IS NOT NULL
    AND NOT latitude BETWEEN -90 AND 90
  ) AS invalid_latitude,

  COUNTIF(
    longitude IS NOT NULL
    AND NOT longitude BETWEEN -180 AND 180
  ) AS invalid_longitude
FROM `jcdeah-009.nyc311_finpro_hita_intermediate.clean_311_streaming`;


-- ========================================
-- 6. NEGATIVE RESOLUTION TIME
-- ========================================

SELECT
  COUNTIF(
    resolution_time_hours < 0
  ) AS negative_resolution_time
FROM `jcdeah-009.nyc311_finpro_hita_intermediate.clean_311_streaming`;
