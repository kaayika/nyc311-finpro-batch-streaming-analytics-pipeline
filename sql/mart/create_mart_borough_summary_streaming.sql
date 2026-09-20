CREATE OR REPLACE TABLE
  `jcdeah-009.nyc311_finpro_hita_mart.mart_borough_summary_streaming`
AS

SELECT
  borough,

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
  borough

ORDER BY
  total_requests DESC;
