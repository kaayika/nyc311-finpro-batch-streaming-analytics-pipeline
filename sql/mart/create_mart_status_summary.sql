CREATE OR REPLACE TABLE
  `jcdeah-009.nyc311_finpro_hita_mart.mart_status_summary`
AS

SELECT
  'CLOSED' AS status,
  SUM(closed_requests) AS total_requests
FROM
  `jcdeah-009.nyc311_finpro_hita_mart.mart_complaint_summary`

UNION ALL

SELECT
  'OPEN' AS status,
  SUM(open_requests) AS total_requests
FROM
  `jcdeah-009.nyc311_finpro_hita_mart.mart_complaint_summary`

UNION ALL

SELECT
  'OTHER' AS status,
  SUM(total_requests)
    - SUM(closed_requests)
    - SUM(open_requests) AS total_requests
FROM
  `jcdeah-009.nyc311_finpro_hita_mart.mart_complaint_summary`;