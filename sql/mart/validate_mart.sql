-- ========================================
-- 1. MART ROW COUNT CHECK
-- ========================================

SELECT
  (SELECT COUNT(*)
   FROM `jcdeah-009.nyc311_finpro_hita_mart.mart_complaint_summary`)
    AS complaint_mart_rows,

  (SELECT COUNT(*)
   FROM `jcdeah-009.nyc311_finpro_hita_mart.mart_borough_summary`)
    AS borough_mart_rows,

  (SELECT COUNT(*)
   FROM `jcdeah-009.nyc311_finpro_hita_mart.mart_agency_performance`)
    AS agency_mart_rows,

  (SELECT COUNT(*)
   FROM `jcdeah-009.nyc311_finpro_hita_mart.mart_daily_trend`)
    AS daily_mart_rows;


-- ========================================
-- 2. TOTAL REQUEST CONSISTENCY CHECK
-- ========================================

WITH source_count AS (

  SELECT

    (
      SELECT COUNT(*)
      FROM `jcdeah-009.nyc311_finpro_hita_intermediate.clean_311_batch`
    ) AS batch_rows,

    (
      SELECT COUNT(*)
      FROM `jcdeah-009.nyc311_finpro_hita_staging.raw_311_streaming`
      WHERE
        NULLIF(TRIM(unique_key), '') IS NOT NULL
        AND created_date IS NOT NULL
        AND created_date >= TIMESTAMP('2026-04-01')
        AND created_date < TIMESTAMP('2026-05-01')
    ) AS streaming_rows
)

SELECT

  batch_rows,

  streaming_rows,

  batch_rows + streaming_rows
    AS expected_total_requests,

  (
    SELECT SUM(total_requests)
    FROM `jcdeah-009.nyc311_finpro_hita_mart.mart_complaint_summary`
  ) AS complaint_total_requests,

  (
    SELECT SUM(total_requests)
    FROM `jcdeah-009.nyc311_finpro_hita_mart.mart_borough_summary`
  ) AS borough_total_requests,

  (
    SELECT SUM(total_requests)
    FROM `jcdeah-009.nyc311_finpro_hita_mart.mart_agency_performance`
  ) AS agency_total_requests,

  (
    SELECT SUM(total_requests)
    FROM `jcdeah-009.nyc311_finpro_hita_mart.mart_daily_trend`
  ) AS daily_total_requests

FROM
  source_count;


-- ========================================
-- 3. DUPLICATE DIMENSION CHECK
-- ========================================

SELECT
  (SELECT COUNT(*) - COUNT(DISTINCT complaint_type)
   FROM `jcdeah-009.nyc311_finpro_hita_mart.mart_complaint_summary`)
    AS duplicate_complaint_type,

  (SELECT COUNT(*) - COUNT(DISTINCT borough)
   FROM `jcdeah-009.nyc311_finpro_hita_mart.mart_borough_summary`)
    AS duplicate_borough,

  (SELECT COUNT(*) - COUNT(DISTINCT agency)
   FROM `jcdeah-009.nyc311_finpro_hita_mart.mart_agency_performance`)
    AS duplicate_agency,

  (SELECT COUNT(*) - COUNT(DISTINCT created_date)
   FROM `jcdeah-009.nyc311_finpro_hita_mart.mart_daily_trend`)
    AS duplicate_created_date;


-- ========================================
-- 4. INVALID METRIC CHECK
-- ========================================

SELECT
  'complaint_summary' AS mart_name,

  COUNTIF(
    total_requests < 0
    OR closed_requests < 0
    OR open_requests < 0
    OR closed_requests + open_requests > total_requests
    OR avg_resolution_time_hours < 0
  ) AS invalid_rows

FROM
  `jcdeah-009.nyc311_finpro_hita_mart.mart_complaint_summary`

UNION ALL

SELECT
  'borough_summary',

  COUNTIF(
    total_requests < 0
    OR closed_requests < 0
    OR open_requests < 0
    OR closed_requests + open_requests > total_requests
    OR avg_resolution_time_hours < 0
  )

FROM
  `jcdeah-009.nyc311_finpro_hita_mart.mart_borough_summary`

UNION ALL

SELECT
  'agency_performance',

  COUNTIF(
    total_requests < 0
    OR closed_requests < 0
    OR open_requests < 0
    OR closed_requests + open_requests > total_requests
    OR avg_resolution_time_hours < 0
  )

FROM
  `jcdeah-009.nyc311_finpro_hita_mart.mart_agency_performance`

UNION ALL

SELECT
  'daily_trend',

  COUNTIF(
    total_requests < 0
    OR closed_requests < 0
    OR open_requests < 0
    OR closed_requests + open_requests > total_requests
    OR avg_resolution_time_hours < 0
  )

FROM
  `jcdeah-009.nyc311_finpro_hita_mart.mart_daily_trend`;


-- ========================================
-- 5. MART SCHEMA CHECK
-- ========================================

SELECT
  table_name,
  column_name,
  data_type
FROM
  `jcdeah-009.nyc311_finpro_hita_mart.INFORMATION_SCHEMA.COLUMNS`
WHERE
  table_name IN (
    'mart_complaint_summary',
    'mart_borough_summary',
    'mart_agency_performance',
    'mart_daily_trend'
  )
ORDER BY
  table_name,
  ordinal_position;


-- ========================================
-- 6. DAILY TREND DATE RANGE CHECK
-- ========================================

SELECT
  COUNT(*) AS total_days,
  MIN(created_date) AS min_date,
  MAX(created_date) AS max_date,
  SUM(total_requests) AS total_requests
FROM
  `jcdeah-009.nyc311_finpro_hita_mart.mart_daily_trend`;