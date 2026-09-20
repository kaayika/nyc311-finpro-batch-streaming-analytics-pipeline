MERGE
  `jcdeah-009.nyc311_finpro_hita_staging.raw_311_batch` AS target

USING
  `jcdeah-009.nyc311_finpro_hita_staging.raw_311_batch_daily_load` AS source

ON target.unique_key = source.unique_key

WHEN MATCHED THEN
  UPDATE SET
    created_date = source.created_date,
    closed_date = source.closed_date,
    agency = source.agency,
    agency_name = source.agency_name,
    complaint_type = source.complaint_type,
    descriptor = source.descriptor,
    status = source.status,
    borough = source.borough,
    incident_zip = source.incident_zip,
    latitude = source.latitude,
    longitude = source.longitude,
    resolution_description = source.resolution_description

WHEN NOT MATCHED THEN
  INSERT (
    unique_key,
    created_date,
    closed_date,
    agency,
    agency_name,
    complaint_type,
    descriptor,
    status,
    borough,
    incident_zip,
    latitude,
    longitude,
    resolution_description
  )
  VALUES (
    source.unique_key,
    source.created_date,
    source.closed_date,
    source.agency,
    source.agency_name,
    source.complaint_type,
    source.descriptor,
    source.status,
    source.borough,
    source.incident_zip,
    source.latitude,
    source.longitude,
    source.resolution_description
  );
