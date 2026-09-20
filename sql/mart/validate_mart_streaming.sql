-- ========================================
-- 1. MART ROW COUNT
-- ========================================

SELECT
  (SELECT COUNT(*)
   FROM `jcdeah-009.nyc311_finpro_hita_mart.mart_complaint_summary_streaming`)
    AS complaint_mart_rows,

  (SELECT COUNT(*)
   FROM `jcdeah-009.nyc311_finpro_hita_mart.mart_borough_summary_streaming`)
    AS borough_mart_rows,

  (SELECT COUNT(*)
   FROM `jcdeah-009.nyc311_finpro_hita_mart.mart_agency_performance_streaming`)
    AS agency_mart_rows;


-- ========================================
-- 2. TOTAL REQUEST CONSISTENCY
-- ========================================

SELECT
  (SELECT COUNT(*)
   FROM `jcdeah-009.nyc311_finpro_hita_intermediate.clean_311_streaming`)
    AS intermediate_rows,

  (SELECT SUM(total_requests)
   FROM `jcdeah-009.nyc311_finpro_hita_mart.mart_complaint_summary_streaming`)
    AS complaint_total_requests,

  (SELECT SUM(total_requests)
   FROM `jcdeah-009.nyc311_finpro_hita_mart.mart_borough_summary_streaming`)
    AS borough_total_requests,

  (SELECT SUM(total_requests)
   FROM `jcdeah-009.nyc311_finpro_hita_mart.mart_agency_performance_streaming`)
    AS agency_total_requests;


-- ========================================
-- 3. DUPLICATE DIMENSION
-- ========================================

SELECT
  (SELECT COUNT(*) - COUNT(DISTINCT complaint_type)
   FROM `jcdeah-009.nyc311_finpro_hita_mart.mart_complaint_summary_streaming`)
    AS duplicate_complaint_type,

  (SELECT COUNT(*) - COUNT(DISTINCT borough)
   FROM `jcdeah-009.nyc311_finpro_hita_mart.mart_borough_summary_streaming`)
    AS duplicate_borough,

  (SELECT COUNT(*) - COUNT(DISTINCT agency)
   FROM `jcdeah-009.nyc311_finpro_hita_mart.mart_agency_performance_streaming`)
    AS duplicate_agency;


-- ========================================
-- 4. INVALID METRIC
-- ========================================

SELECT

  (SELECT COUNTIF(
      total_requests < 0
      OR closed_requests < 0
      OR open_requests < 0
      OR closed_requests + open_requests > total_requests
      OR avg_resolution_time_hours < 0
   )
   FROM `jcdeah-009.nyc311_finpro_hita_mart.mart_complaint_summary_streaming`)
    AS invalid_complaint_metric,

  (SELECT COUNTIF(
      total_requests < 0
      OR closed_requests < 0
      OR open_requests < 0
      OR closed_requests + open_requests > total_requests
      OR avg_resolution_time_hours < 0
   )
   FROM `jcdeah-009.nyc311_finpro_hita_mart.mart_borough_summary_streaming`)
    AS invalid_borough_metric,

  (SELECT COUNTIF(
      total_requests < 0
      OR closed_requests < 0
      OR open_requests < 0
      OR closed_requests + open_requests > total_requests
      OR avg_resolution_time_hours < 0
   )
   FROM `jcdeah-009.nyc311_finpro_hita_mart.mart_agency_performance_streaming`)
    AS invalid_agency_metric;
