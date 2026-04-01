from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

DEFAULT_ARGS = {
    "owner": "KHADIJA",
    "start_date": datetime(2026, 3, 30),
}

with DAG(
    dag_id="elt_dag",
    default_args=DEFAULT_ARGS,
    description="DAG ELT pour Ticketmaster",
    schedule="@daily",
    catchup=False,
) as dag:
    run_producer2026 = BashOperator(
        task_id="run_producer2026",
        bash_command="python3 /opt/airflow/scripts/producer2026.py",
    )

    run_producer2027 = BashOperator(
        task_id="run_producer2027",
        bash_command="python3 /opt/airflow/scripts/producer2027.py",
    )

    run_consumer = BashOperator(
        task_id="run_consumer",
        bash_command="python3 /opt/airflow/scripts/consumer.py",
    )

    run_dbt = BashOperator(
    task_id="run_dbt",
    bash_command=(
        "cd /opt/airflow/dbt/project_ticketmaster && "
        "dbt run --profiles-dir . --target dev"
    ),
)
    [run_producer2026 , run_producer2027] >> run_consumer >> run_dbt