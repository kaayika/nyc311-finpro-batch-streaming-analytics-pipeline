CREATE OR REPLACE TABLE
  `jcdeah-009.nyc311_finpro_hita_mart.mart_agency_performance_streaming`
AS

SELECT
  agency,

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
  `jcdeah-009.nyc311_finpro_hita_intermediate.clean_311_streaming`

GROUP BY
  agency

ORDER BY
  total_requests DESC;
