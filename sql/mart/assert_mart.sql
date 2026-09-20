-- ========================================
-- MART DATA QUALITY ASSERTIONS
-- ========================================

-- 1. MART TABLES MUST NOT BE EMPTY
ASSERT (
  SELECT
    (
      SELECT COUNT(*)
      FROM `jcdeah-009.nyc311_finpro_hita_mart.mart_complaint_summary`
    ) > 0

    AND

    (
      SELECT COUNT(*)
      FROM `jcdeah-009.nyc311_finpro_hita_mart.mart_borough_summary`
    ) > 0

    AND

    (
      SELECT COUNT(*)
      FROM `jcdeah-009.nyc311_finpro_hita_mart.mart_agency_performance`
    ) > 0
)
AS 'One or more Mart tables are empty';


-- 2. TOTAL REQUEST CONSISTENCY
ASSERT (
  SELECT
    (
      SELECT SUM(total_requests)
      FROM `jcdeah-009.nyc311_finpro_hita_mart.mart_complaint_summary`
    )
    =
    (
      SELECT COUNT(*)
      FROM `jcdeah-009.nyc311_finpro_hita_intermediate.clean_311_batch`
    )

    AND

    (
      SELECT SUM(total_requests)
      FROM `jcdeah-009.nyc311_finpro_hita_mart.mart_borough_summary`
    )
    =
    (
      SELECT COUNT(*)
      FROM `jcdeah-009.nyc311_finpro_hita_intermediate.clean_311_batch`
    )

    AND

    (
      SELECT SUM(total_requests)
      FROM `jcdeah-009.nyc311_finpro_hita_mart.mart_agency_performance`
    )
    =
    (
      SELECT COUNT(*)
      FROM `jcdeah-009.nyc311_finpro_hita_intermediate.clean_311_batch`
    )
)
AS 'Mart total request does not match intermediate';


-- 3. DUPLICATE DIMENSION
ASSERT (
  SELECT
    (
      SELECT COUNT(*) - COUNT(DISTINCT complaint_type)
      FROM `jcdeah-009.nyc311_finpro_hita_mart.mart_complaint_summary`
    ) = 0

    AND

    (
      SELECT COUNT(*) - COUNT(DISTINCT borough)
      FROM `jcdeah-009.nyc311_finpro_hita_mart.mart_borough_summary`
    ) = 0

    AND

    (
      SELECT COUNT(*) - COUNT(DISTINCT agency)
      FROM `jcdeah-009.nyc311_finpro_hita_mart.mart_agency_performance`
    ) = 0
)
AS 'Duplicate dimension found in Mart';


-- 4. INVALID METRIC
ASSERT (
  SELECT

    (
      SELECT COUNTIF(
        total_requests < 0
        OR closed_requests < 0
        OR open_requests < 0
        OR closed_requests + open_requests > total_requests
        OR avg_resolution_time_hours < 0
      )
      FROM `jcdeah-009.nyc311_finpro_hita_mart.mart_complaint_summary`
    ) = 0

    AND

    (
      SELECT COUNTIF(
        total_requests < 0
        OR closed_requests < 0
        OR open_requests < 0
        OR closed_requests + open_requests > total_requests
        OR avg_resolution_time_hours < 0
      )
      FROM `jcdeah-009.nyc311_finpro_hita_mart.mart_borough_summary`
    ) = 0

    AND

    (
      SELECT COUNTIF(
        total_requests < 0
        OR closed_requests < 0
        OR open_requests < 0
        OR closed_requests + open_requests > total_requests
        OR avg_resolution_time_hours < 0
      )
      FROM `jcdeah-009.nyc311_finpro_hita_mart.mart_agency_performance`
    ) = 0
)
AS 'Invalid metric found in Mart';
