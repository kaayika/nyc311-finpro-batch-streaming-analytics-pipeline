CREATE OR REPLACE TABLE
  `jcdeah-009.nyc311_finpro_hita_mart.mart_complaint_summary`
AS

-- ========================================
-- CLEAN STREAMING DATA
-- ========================================

WITH streaming_clean AS (

  SELECT

    NULLIF(
      LOWER(TRIM(complaint_type)),
      ''
    ) AS complaint_type,

    NULLIF(
      UPPER(TRIM(status)),
      ''
    ) AS status,

    CASE
      WHEN closed_date IS NOT NULL
        AND closed_date >= created_date
      THEN ROUND(
        TIMESTAMP_DIFF(
          closed_date,
          created_date,
          MINUTE
        ) / 60.0,
        2
      )
      ELSE NULL
    END AS resolution_time_hours

  FROM
    `jcdeah-009.nyc311_finpro_hita_staging.raw_311_streaming`

  WHERE
    NULLIF(TRIM(unique_key), '') IS NOT NULL

    AND created_date IS NOT NULL

    AND created_date >= TIMESTAMP('2026-04-01')

    AND created_date < TIMESTAMP('2026-05-01')
),


-- ========================================
-- COMBINE BATCH + STREAMING
-- ========================================

combined AS (

  SELECT
    complaint_type,
    status,
    resolution_time_hours

  FROM
    `jcdeah-009.nyc311_finpro_hita_intermediate.clean_311_batch`

  UNION ALL

  SELECT
    complaint_type,
    status,
    resolution_time_hours

  FROM
    streaming_clean
)


-- ========================================
-- CREATE COMPLAINT SUMMARY MART
-- ========================================

SELECT

  complaint_type,

  COUNT(*) AS total_requests,

  COUNTIF(
    status = 'CLOSED'
  ) AS closed_requests,

  COUNTIF(
    status = 'OPEN'
  ) AS open_requests,

  ROUND(
    AVG(resolution_time_hours),
    2
  ) AS avg_resolution_time_hours

FROM
  combined

GROUP BY
  complaint_type

ORDER BY
  total_requests DESC;