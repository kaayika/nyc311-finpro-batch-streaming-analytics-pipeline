CREATE OR REPLACE TABLE
  `jcdeah-009.nyc311_finpro_hita_intermediate.clean_311_batch`

PARTITION BY DATE(created_date)
CLUSTER BY borough, agency, complaint_type

AS

-- ========================================
-- PREPARE TIMESTAMP COLUMNS
-- ========================================

WITH source AS (

 SELECT
    *,

    created_date AS created_ts,

    closed_date AS closed_ts

  FROM
    `jcdeah-009.nyc311_finpro_hita_staging.raw_311_batch`
)


SELECT

  -- ========================================
  -- ORIGINAL / CLEANED COLUMNS
  -- ========================================

  NULLIF(TRIM(unique_key), '') AS unique_key,

  created_ts AS created_date,

  CASE
    WHEN closed_ts >= created_ts
      THEN closed_ts
    ELSE NULL
  END AS closed_date,

  NULLIF(
    UPPER(TRIM(agency)),
    ''
  ) AS agency,

  NULLIF(
    TRIM(agency_name),
    ''
  ) AS agency_name,

  NULLIF(
    LOWER(TRIM(complaint_type)),
    ''
  ) AS complaint_type,

  NULLIF(
    TRIM(descriptor),
    ''
  ) AS descriptor,

  NULLIF(
    UPPER(TRIM(status)),
    ''
  ) AS status,

  NULLIF(
    UPPER(TRIM(borough)),
    ''
  ) AS borough,

  NULLIF(
    TRIM(incident_zip),
    ''
  ) AS incident_zip,


  -- ========================================
  -- COORDINATE VALIDATION
  -- ========================================

  CASE
    WHEN latitude BETWEEN -90 AND 90
      THEN latitude
    ELSE NULL
  END AS latitude,

  CASE
    WHEN longitude BETWEEN -180 AND 180
      THEN longitude
    ELSE NULL
  END AS longitude,

  NULLIF(
    TRIM(resolution_description),
    ''
  ) AS resolution_description,


  -- ========================================
  -- DERIVED DATE COLUMNS
  -- ========================================

  DATE(created_ts) AS created_date_only,

  EXTRACT(
    YEAR FROM created_ts
  ) AS created_year,

  EXTRACT(
    MONTH FROM created_ts
  ) AS created_month,

  EXTRACT(
    DAY FROM created_ts
  ) AS created_day,

  EXTRACT(
    HOUR FROM created_ts
  ) AS created_hour,


  -- ========================================
  -- RESOLUTION TIME
  -- ========================================

  CASE
    WHEN closed_ts IS NOT NULL
      AND closed_ts >= created_ts
    THEN ROUND(
      TIMESTAMP_DIFF(
        closed_ts,
        created_ts,
        MINUTE
      ) / 60.0,
      2
    )
    ELSE NULL
  END AS resolution_time_hours


FROM
  source

WHERE
  NULLIF(TRIM(unique_key), '') IS NOT NULL

  AND created_ts IS NOT NULL

  AND created_ts >= TIMESTAMP('2026-01-01')

  AND created_ts < TIMESTAMP('2026-04-01');
