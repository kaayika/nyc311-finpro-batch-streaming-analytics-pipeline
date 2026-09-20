from datetime import timedelta
from pathlib import Path

import pendulum

from airflow.sdk import DAG

from airflow.providers.apache.beam.operators.beam import (
    BeamRunPythonPipelineOperator,
)

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)

from airflow.providers.smtp.notifications.smtp import (
    send_smtp_notification,
)

# ========================================
# PROJECT CONFIGURATION
# ========================================

PROJECT_ID = "jcdeah-009"
REGION = "asia-southeast2"

BUCKET = "jcdeah-009-nyc311-final-project-hita"

STAGING_DATASET = "nyc311_finpro_hita_staging"

SQL_DIR = Path("/opt/airflow/sql")

DATAFLOW_SCRIPT = "/opt/airflow/scripts/batch/dataflow_gcs_to_bq.py"


# ========================================
# SQL READER
# ========================================


def read_sql(relative_path):
    return (SQL_DIR / relative_path).read_text()


# ========================================
# FAILURE NOTIFICATION
# ========================================
failure_email = send_smtp_notification(
    smtp_conn_id="smtp_default",
    from_email="hita.koribali@gmail.com",
    to="hita.koribali@gmail.com",
    subject="[FAILED] NYC 311 Batch Pipeline",
    html_content="""
    <h3>NYC 311 Batch Pipeline Failed</h3>
    <p>DAG : {{ dag.dag_id }}</p>
    <p>Task : {{ ti.task_id }}</p>
    <p>Run ID : {{ run_id }}</p>
    """,
)


# ========================================
# DEFAULT ARGUMENTS
# ========================================

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": [failure_email],
}


# ========================================
# DAG
# ========================================

with DAG(
    dag_id="nyc311_batch_pipeline",
    description="NYC 311 Batch Pipeline from GCS via Dataflow to BigQuery",
    start_date=pendulum.datetime(
        2026,
        9,
        1,
        tz="Asia/Jakarta",
    ),
    schedule="0 2 1 * *",
    catchup=False,
    default_args=default_args,
    tags=["nyc311", "batch", "final-project"],
) as dag:

    # ========================================
    # 1. RUN DATAFLOW BATCH
    # ========================================

    run_dataflow_batch = BeamRunPythonPipelineOperator(
        task_id="run_dataflow_batch",
        runner="DataflowRunner",
        py_file=DATAFLOW_SCRIPT,
        py_interpreter="python3",
        pipeline_options={
            "project": PROJECT_ID,
            "region": REGION,
            # Pakai zone B karena sebelumnya A dan C
            # mengalami RESOURCE_POOL_EXHAUSTED
            "worker_zone": "asia-southeast2-b",
            "temp_location": (f"gs://{BUCKET}/temp/dataflow/"),
            "staging_location": (f"gs://{BUCKET}/staging/dataflow/"),
            "input": (f"gs://{BUCKET}/raw/batch/daily/*/*.parquet"),
            "output_table": (f"{PROJECT_ID}:" f"{STAGING_DATASET}." "raw_311_batch"),
            "bq_temp_location": (f"gs://{BUCKET}/temp/dataflow/bq/"),
        },
        dataflow_config={
            "job_name": "nyc311-finpro-hita-batch",
            "location": REGION,
        },
        gcp_conn_id="google_cloud_default",
    )

    # ========================================
    # 2. STAGING DATA QUALITY
    # ========================================

    validate_staging = BigQueryInsertJobOperator(
        task_id="validate_staging",
        configuration={
            "query": {
                "query": read_sql("staging/assert_raw_311_batch.sql"),
                "useLegacySql": False,
            }
        },
        project_id=PROJECT_ID,
        location=REGION,
        gcp_conn_id="google_cloud_default",
    )

    # ========================================
    # 3. TRANSFORM INTERMEDIATE
    # ========================================

    transform_intermediate = BigQueryInsertJobOperator(
        task_id="transform_intermediate",
        configuration={
            "query": {
                "query": read_sql("intermediate/create_clean_311_batch.sql"),
                "useLegacySql": False,
            }
        },
        project_id=PROJECT_ID,
        location=REGION,
        gcp_conn_id="google_cloud_default",
    )

    # ========================================
    # 4. INTERMEDIATE DATA QUALITY
    # ========================================

    validate_intermediate = BigQueryInsertJobOperator(
        task_id="validate_intermediate",
        configuration={
            "query": {
                "query": read_sql("intermediate/assert_clean_311_batch.sql"),
                "useLegacySql": False,
            }
        },
        project_id=PROJECT_ID,
        location=REGION,
        gcp_conn_id="google_cloud_default",
    )

    # ========================================
    # 5. BUILD COMPLAINT MART
    # ========================================

    build_complaint_mart = BigQueryInsertJobOperator(
        task_id="build_complaint_mart",
        configuration={
            "query": {
                "query": read_sql("mart/create_mart_complaint_summary.sql"),
                "useLegacySql": False,
            }
        },
        project_id=PROJECT_ID,
        location=REGION,
        gcp_conn_id="google_cloud_default",
    )

    # ========================================
    # 6. BUILD BOROUGH MART
    # ========================================

    build_borough_mart = BigQueryInsertJobOperator(
        task_id="build_borough_mart",
        configuration={
            "query": {
                "query": read_sql("mart/create_mart_borough_summary.sql"),
                "useLegacySql": False,
            }
        },
        project_id=PROJECT_ID,
        location=REGION,
        gcp_conn_id="google_cloud_default",
    )

    # ========================================
    # 7. BUILD AGENCY MART
    # ========================================

    build_agency_mart = BigQueryInsertJobOperator(
        task_id="build_agency_mart",
        configuration={
            "query": {
                "query": read_sql("mart/create_mart_agency_performance.sql"),
                "useLegacySql": False,
            }
        },
        project_id=PROJECT_ID,
        location=REGION,
        gcp_conn_id="google_cloud_default",
    )

    # ========================================
    # 8. MART DATA QUALITY
    # ========================================

    validate_mart = BigQueryInsertJobOperator(
        task_id="validate_mart",
        configuration={
            "query": {
                "query": read_sql("mart/assert_mart.sql"),
                "useLegacySql": False,
            }
        },
        project_id=PROJECT_ID,
        location=REGION,
        gcp_conn_id="google_cloud_default",
    )

    # ========================================
    # TASK DEPENDENCIES
    # ========================================

    (
        run_dataflow_batch
        >> validate_staging
        >> transform_intermediate
        >> validate_intermediate
    )

    validate_intermediate >> [
        build_complaint_mart,
        build_borough_mart,
        build_agency_mart,
    ]

    [
        build_complaint_mart,
        build_borough_mart,
        build_agency_mart,
    ] >> validate_mart
