"""
dbt Daily DAG - Run dbt models daily
Schedule: Every day at UTC 02:00 (Beijing time 10:00)
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
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
    log_url = context.get('task_instance').log_url

    print(f"[ALERT] DAG Failed!")
    print(f"  DAG: {dag_id}")
    print(f"  Task: {task_id}")
    print(f"  Execution Time: {execution_date}")
    print(f"  Log URL: {log_url}")


with DAG(
    'dbt_daily',
    default_args=default_args,
    description='Daily dbt run for all models',
    schedule_interval='0 2 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dbt', 'daily', 'core'],
    on_failure_callback=notify_failure,
    max_active_runs=1,
) as dag:

    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command=f"""
        cd {DBT_PROJECT_DIR} && \
        dbt deps --profiles-dir {DBT_PROFILES_DIR} --target {DBT_TARGET}
        """,
    )

    with TaskGroup(group_id='staging_models') as staging_group:
        dbt_staging = BashOperator(
            task_id='run_staging',
            bash_command=f"""
            cd {DBT_PROJECT_DIR} && \
            dbt run --select staging --profiles-dir {DBT_PROFILES_DIR} --target {DBT_TARGET}
            """,
        )

        dbt_staging_test = BashOperator(
            task_id='test_staging',
            bash_command=f"""
            cd {DBT_PROJECT_DIR} && \
            dbt test --select staging --profiles-dir {DBT_PROFILES_DIR} --target {DBT_TARGET}
            """,
        )

        dbt_staging >> dbt_staging_test

    with TaskGroup(group_id='core_models') as core_group:
        dbt_core = BashOperator(
            task_id='run_core',
            bash_command=f"""
            cd {DBT_PROJECT_DIR} && \
            dbt run --select core --profiles-dir {DBT_PROFILES_DIR} --target {DBT_TARGET}
            """,
        )

        dbt_core_test = BashOperator(
            task_id='test_core',
            bash_command=f"""
            cd {DBT_PROJECT_DIR} && \
            dbt test --select core --profiles-dir {DBT_PROFILES_DIR} --target {DBT_TARGET}
            """,
        )

        dbt_core >> dbt_core_test

    with TaskGroup(group_id='mart_models') as mart_group:
        dbt_mart = BashOperator(
            task_id='run_mart',
            bash_command=f"""
            cd {DBT_PROJECT_DIR} && \
            dbt run --select mart --profiles-dir {DBT_PROFILES_DIR} --target {DBT_TARGET}
            """,
        )

        dbt_mart_test = BashOperator(
            task_id='test_mart',
            bash_command=f"""
            cd {DBT_PROJECT_DIR} && \
            dbt test --select mart --profiles-dir {DBT_PROFILES_DIR} --target {DBT_TARGET}
            """,
        )

        dbt_mart >> dbt_mart_test

    dbt_docs = BashOperator(
        task_id='dbt_docs_generate',
        bash_command=f"""
        cd {DBT_PROJECT_DIR} && \
        dbt docs generate --profiles-dir {DBT_PROFILES_DIR} --target {DBT_TARGET}
        """,
    )

    notify_success = BashOperator(
        task_id='notify_success',
        bash_command='echo "[SUCCESS] dbt Daily DAG completed successfully at $(date)"',
    )

    dbt_deps >> staging_group >> core_group >> mart_group >> dbt_docs >> notify_success
