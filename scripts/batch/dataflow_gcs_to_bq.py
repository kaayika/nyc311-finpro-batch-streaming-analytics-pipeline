import argparse
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

# ========================================
# BIGQUERY SCHEMA
# ========================================

BQ_SCHEMA = {
    "fields": [
        {"name": "unique_key", "type": "STRING", "mode": "NULLABLE"},
        {"name": "created_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "closed_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "agency", "type": "STRING", "mode": "NULLABLE"},
        {"name": "agency_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "complaint_type", "type": "STRING", "mode": "NULLABLE"},
        {"name": "descriptor", "type": "STRING", "mode": "NULLABLE"},
        {"name": "status", "type": "STRING", "mode": "NULLABLE"},
        {"name": "borough", "type": "STRING", "mode": "NULLABLE"},
        {"name": "incident_zip", "type": "STRING", "mode": "NULLABLE"},
        {"name": "latitude", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "longitude", "type": "FLOAT", "mode": "NULLABLE"},
        {
            "name": "resolution_description",
            "type": "STRING",
            "mode": "NULLABLE",
        },
    ]
}


# ========================================
# REQUIRED COLUMNS
# ========================================

REQUIRED_COLUMNS = [
    "unique_key",
    "created_date",
    "closed_date",
    "agency",
    "agency_name",
    "complaint_type",
    "descriptor",
    "status",
    "borough",
    "incident_zip",
    "latitude",
    "longitude",
    "resolution_description",
]


# ========================================
# PREPARE ROW FOR BIGQUERY
# ========================================


def prepare_row(row):

    result = {}

    for column in REQUIRED_COLUMNS:

        value = row.get(column)

        # Convert timestamp / datetime to ISO string
        if isinstance(value, datetime):
            value = value.isoformat()

        result[column] = value

    return result


# ========================================
# PIPELINE
# ========================================


def run():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input",
        required=True,
        help="GCS Parquet input pattern",
    )

    parser.add_argument(
        "--output_table",
        required=True,
        help="BigQuery destination table",
    )

    parser.add_argument(
        "--bq_temp_location",
        required=True,
        help="Temporary GCS path for BigQuery file loads",
    )

    known_args, pipeline_args = parser.parse_known_args()

    pipeline_options = PipelineOptions(pipeline_args)

    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as pipeline:

        (
            pipeline
            | "Read Parquet from GCS" >> beam.io.ReadFromParquet(known_args.input)
            | "Prepare Rows" >> beam.Map(prepare_row)
            | "Write to BigQuery Staging"
            >> beam.io.WriteToBigQuery(
                table=known_args.output_table,
                schema=BQ_SCHEMA,
                create_disposition=(beam.io.BigQueryDisposition.CREATE_IF_NEEDED),
                write_disposition=(beam.io.BigQueryDisposition.WRITE_TRUNCATE),
                method=(beam.io.WriteToBigQuery.Method.FILE_LOADS),
                custom_gcs_temp_location=(known_args.bq_temp_location),
            )
        )


if __name__ == "__main__":
    run()
