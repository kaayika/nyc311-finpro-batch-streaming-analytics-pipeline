import argparse
import json

import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    SetupOptions,
    StandardOptions,
)


# ========================================
# BIGQUERY SCHEMA
# ========================================

BQ_SCHEMA = {
    "fields": [
        {
            "name": "unique_key",
            "type": "STRING",
            "mode": "NULLABLE",
        },
        {
            "name": "created_date",
            "type": "TIMESTAMP",
            "mode": "NULLABLE",
        },
        {
            "name": "closed_date",
            "type": "TIMESTAMP",
            "mode": "NULLABLE",
        },
        {
            "name": "agency",
            "type": "STRING",
            "mode": "NULLABLE",
        },
        {
            "name": "agency_name",
            "type": "STRING",
            "mode": "NULLABLE",
        },
        {
            "name": "complaint_type",
            "type": "STRING",
            "mode": "NULLABLE",
        },
        {
            "name": "descriptor",
            "type": "STRING",
            "mode": "NULLABLE",
        },
        {
            "name": "status",
            "type": "STRING",
            "mode": "NULLABLE",
        },
        {
            "name": "borough",
            "type": "STRING",
            "mode": "NULLABLE",
        },
        {
            "name": "incident_zip",
            "type": "STRING",
            "mode": "NULLABLE",
        },
        {
            "name": "latitude",
            "type": "FLOAT",
            "mode": "NULLABLE",
        },
        {
            "name": "longitude",
            "type": "FLOAT",
            "mode": "NULLABLE",
        },
        {
            "name": "resolution_description",
            "type": "STRING",
            "mode": "NULLABLE",
        },
    ]
}


# ========================================
# STRING COLUMNS
# ========================================

STRING_COLUMNS = [
    "unique_key",
    "agency",
    "agency_name",
    "complaint_type",
    "descriptor",
    "status",
    "borough",
    "incident_zip",
    "resolution_description",
]


# ========================================
# PARSE PUB/SUB MESSAGE
# ========================================

def parse_message(message):

    row = json.loads(
        message.decode("utf-8")
    )


    # ========================================
    # STRING VALUES
    # ========================================

    for column in STRING_COLUMNS:

        value = row.get(column)

        if value is not None:
            row[column] = str(value)


    # ========================================
    # TIMESTAMP VALUES
    # ========================================

    for column in [
        "created_date",
        "closed_date",
    ]:

        value = row.get(column)

        if value in [
            None,
            "",
        ]:
            row[column] = None


    # ========================================
    # FLOAT VALUES
    # ========================================

    for column in [
        "latitude",
        "longitude",
    ]:

        value = row.get(column)

        if value in [
            None,
            "",
        ]:
            row[column] = None

        else:
            row[column] = float(value)


    return row


# ========================================
# PIPELINE
# ========================================

def run():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--subscription",
        required=True,
        help="Pub/Sub subscription path",
    )

    parser.add_argument(
        "--output_table",
        required=True,
        help="BigQuery destination table",
    )

    known_args, pipeline_args = (
        parser.parse_known_args()
    )


    # ========================================
    # PIPELINE OPTIONS
    # ========================================

    pipeline_options = PipelineOptions(
        pipeline_args
    )

    pipeline_options.view_as(
        StandardOptions
    ).streaming = True

    pipeline_options.view_as(
        SetupOptions
    ).save_main_session = True


    # ========================================
    # BUILD PIPELINE
    # ========================================

    with beam.Pipeline(
        options=pipeline_options
    ) as pipeline:

        (
            pipeline

            | "Read from PubSub"
            >> beam.io.ReadFromPubSub(
                subscription=(
                    known_args.subscription
                )
            )

            | "Parse JSON Message"
            >> beam.Map(
                parse_message
            )

            | "Write to BigQuery Staging"
            >> beam.io.WriteToBigQuery(
                table=known_args.output_table,
                schema=BQ_SCHEMA,

                create_disposition=(
                    beam.io.BigQueryDisposition
                    .CREATE_NEVER
                ),

                write_disposition=(
                    beam.io.BigQueryDisposition
                    .WRITE_APPEND
                ),

                method=(
                    beam.io.WriteToBigQuery
                    .Method.STREAMING_INSERTS
                ),
            )
        )


if __name__ == "__main__":
    run()
