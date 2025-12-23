"""
dbt Hourly DAG - Run incremental dbt models hourly
Schedule: Every hour at minute 15
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'execution_timeout': timedelta(minutes=30),
}

DBT_PROJECT_DIR = '/opt/airflow/dbt_base_data'
DBT_PROFILES_DIR = '/opt/airflow/dbt_base_data'
DBT_TARGET = 'prod'


def notify_failure(context):
    """Log failure notification"""
    task_instance = context.get('task_instance')
    dag_id = context.get('task_instance').dag_id
    task_id = context.get('task_instance').task_id
    execution_date = context.get('execution_date')

    print(f"[ALERT] Hourly DAG Failed!")
    print(f"  DAG: {dag_id}")
    print(f"  Task: {task_id}")
    print(f"  Execution Time: {execution_date}")


with DAG(
    'dbt_hourly',
    default_args=default_args,
    description='Hourly dbt run for incremental models',
    schedule_interval='15 * * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dbt', 'hourly', 'incremental'],
    on_failure_callback=notify_failure,
    max_active_runs=1,
) as dag:

    run_incremental_staging = BashOperator(
        task_id='run_incremental_staging',
        bash_command=f"""
        cd {DBT_PROJECT_DIR} && \
        dbt run --select staging --profiles-dir {DBT_PROFILES_DIR} --target {DBT_TARGET}
        """,
    )

    run_incremental_core = BashOperator(
        task_id='run_incremental_core',
        bash_command=f"""
        cd {DBT_PROJECT_DIR} && \
        dbt run --select core.core_address_positions core.core_address_netflow_daily \
        --profiles-dir {DBT_PROFILES_DIR} --target {DBT_TARGET}
        """,
    )

    run_incremental_mart = BashOperator(
        task_id='run_incremental_mart',
        bash_command=f"""
        cd {DBT_PROJECT_DIR} && \
        dbt run --select mart.mart_whale_movements mart.mart_ecosystem_kpis \
        --profiles-dir {DBT_PROFILES_DIR} --target {DBT_TARGET}
        """,
    )

    notify_success = BashOperator(
        task_id='notify_success',
        bash_command='echo "[SUCCESS] dbt Hourly DAG completed at $(date)"',
    )

    run_incremental_staging >> run_incremental_core >> run_incremental_mart >> notify_success
