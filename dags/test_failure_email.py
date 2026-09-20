from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.smtp.notifications.smtp import (
    send_smtp_notification,
)


failure_email = send_smtp_notification(
    smtp_conn_id="smtp_default",
    from_email="hita.koribali@gmail.com",
    to="hita.koribali@gmail.com",
    subject="[TEST FAILED] NYC 311 Airflow Alert",
    html_content="""
    <h3>Failure Alert Test</h3>
    <p>DAG : {{ dag.dag_id }}</p>
    <p>Task : {{ ti.task_id }}</p>
    <p>Run ID : {{ run_id }}</p>
    """,
)


with DAG(
    dag_id="test_failure_email",
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
) as dag:

    test_failure = BashOperator(
        task_id="test_failure",
        bash_command="exit 1",
        retries=0,
        on_failure_callback=[failure_email],
    )
