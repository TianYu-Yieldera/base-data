"""
Backfill DAG - Manual data backfill
Trigger: Manual trigger with parameters
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models.param import Param

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(hours=6),
}

DBT_PROJECT_DIR = '/opt/airflow/dbt_base_data'
DBT_PROFILES_DIR = '/opt/airflow/dbt_base_data'

with DAG(
    'dbt_backfill',
    default_args=default_args,
    description='Manual dbt backfill DAG',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dbt', 'backfill', 'manual'],
    params={
        'start_date': Param(
            default='2024-01-01',
            type='string',
            description='Backfill start date (YYYY-MM-DD)',
        ),
        'end_date': Param(
            default='2024-01-31',
            type='string',
            description='Backfill end date (YYYY-MM-DD)',
        ),
        'models': Param(
            default='core',
            type='string',
            description='dbt models to backfill (staging/core/mart or model name)',
        ),
        'full_refresh': Param(
            default=False,
            type='boolean',
            description='Run with --full-refresh flag',
        ),
    },
) as dag:

    log_params = BashOperator(
        task_id='log_params',
        bash_command='''
        echo "=== Backfill Parameters ==="
        echo "Start Date: {{ params.start_date }}"
        echo "End Date: {{ params.end_date }}"
        echo "Models: {{ params.models }}"
        echo "Full Refresh: {{ params.full_refresh }}"
        echo "=========================="
        ''',
    )

    dbt_backfill = BashOperator(
        task_id='dbt_backfill',
        bash_command=f'''
        cd {DBT_PROJECT_DIR} && \
        dbt run \
            --select {{{{ params.models }}}} \
            --profiles-dir {DBT_PROFILES_DIR} \
            --target prod \
            --vars '{{"start_date": "{{{{ params.start_date }}}}", "end_date": "{{{{ params.end_date }}}}"}}' \
            {{% if params.full_refresh %}}--full-refresh{{% endif %}}
        ''',
    )

    dbt_test = BashOperator(
        task_id='dbt_test_backfill',
        bash_command=f'''
        cd {DBT_PROJECT_DIR} && \
        dbt test --select {{{{ params.models }}}} --profiles-dir {DBT_PROFILES_DIR} --target prod
        ''',
    )

    notify_complete = BashOperator(
        task_id='notify_complete',
        bash_command='''
        echo "[SUCCESS] Backfill completed at $(date)"
        echo "Models: {{ params.models }}"
        echo "Date Range: {{ params.start_date }} to {{ params.end_date }}"
        ''',
    )

    log_params >> dbt_backfill >> dbt_test >> notify_complete
