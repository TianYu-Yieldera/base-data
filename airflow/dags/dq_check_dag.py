"""
Data Quality Check DAG - Check data freshness, completeness, consistency
Schedule: Every hour
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def get_clickhouse_client():
    """Get ClickHouse connection"""
    import clickhouse_driver
    return clickhouse_driver.Client(
        host='clickhouse',
        port=9000,
        user='default',
        password='',
        database='base_data',
    )


def check_data_freshness(**context):
    """Check data freshness"""
    client = get_clickhouse_client()

    checks = [
        {
            'name': 'raw_blocks_freshness',
            'query': '''
                SELECT toInt64(now() - max(toDateTime(timestamp / 1000))) as delay_seconds
                FROM raw_base_blocks
            ''',
            'threshold_seconds': 300,
        },
        {
            'name': 'core_positions_freshness',
            'query': '''
                SELECT toInt64(now() - max(dbt_updated_at)) as delay_seconds
                FROM core_address_positions FINAL
            ''',
            'threshold_seconds': 86400,
        },
    ]

    alerts = []
    for check in checks:
        try:
            result = client.execute(check['query'])
            delay = result[0][0] if result and result[0][0] is not None else float('inf')

            if delay > check['threshold_seconds']:
                alerts.append({
                    'check': check['name'],
                    'delay': delay,
                    'threshold': check['threshold_seconds'],
                })
                print(f"[WARN] Freshness check failed: {check['name']}, delay={delay}s, threshold={check['threshold_seconds']}s")
            else:
                print(f"[OK] Freshness check passed: {check['name']}, delay={delay}s")
        except Exception as e:
            print(f"[ERROR] Freshness check error: {check['name']}, error={str(e)}")
            alerts.append({
                'check': check['name'],
                'error': str(e),
            })

    if alerts:
        context['ti'].xcom_push(key='freshness_alerts', value=alerts)
        raise ValueError(f"Data freshness check failed: {alerts}")

    return "All freshness checks passed"


def check_data_completeness(**context):
    """Check data completeness"""
    client = get_clickhouse_client()

    checks = [
        {
            'name': 'positions_not_null',
            'query': '''
                SELECT
                    count(*) as total,
                    countIf(health_factor IS NULL) as null_hf,
                    countIf(total_collateral_usd IS NULL) as null_collateral
                FROM core_address_positions FINAL
            ''',
            'null_threshold': 0.01,
        },
    ]

    alerts = []
    for check in checks:
        try:
            result = client.execute(check['query'])
            total, null_hf, null_collateral = result[0]

            if total == 0:
                print(f"[WARN] No data found for: {check['name']}")
                continue

            null_rate = (null_hf + null_collateral) / (total * 2) if total > 0 else 1
            if null_rate > check['null_threshold']:
                alerts.append({
                    'check': check['name'],
                    'null_rate': null_rate,
                    'threshold': check['null_threshold'],
                })
                print(f"[WARN] Completeness check failed: {check['name']}, null_rate={null_rate:.2%}")
            else:
                print(f"[OK] Completeness check passed: {check['name']}, null_rate={null_rate:.2%}")
        except Exception as e:
            print(f"[ERROR] Completeness check error: {check['name']}, error={str(e)}")

    if alerts:
        context['ti'].xcom_push(key='completeness_alerts', value=alerts)
        raise ValueError(f"Data completeness check failed: {alerts}")

    return "All completeness checks passed"


def check_data_consistency(**context):
    """Check data consistency"""
    client = get_clickhouse_client()

    try:
        result = client.execute('''
            SELECT
                count(*) as total,
                countIf(health_factor < 0) as negative_hf,
                countIf(health_factor > 1000) as extreme_hf
            FROM core_address_positions FINAL
            WHERE dbt_updated_at >= now() - INTERVAL 1 DAY
        ''')

        total, negative, extreme = result[0]
        print(f"[INFO] Consistency check: total={total}, negative_hf={negative}, extreme_hf={extreme}")

        if negative > 0 or extreme > 0:
            raise ValueError(f"Data consistency check failed: {negative} negative HF, {extreme} extreme HF")

        print("[OK] All consistency checks passed")
        return "All consistency checks passed"

    except Exception as e:
        print(f"[ERROR] Consistency check error: {str(e)}")
        raise


with DAG(
    'dq_check',
    default_args=default_args,
    description='Data quality checks',
    schedule_interval='0 * * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dq', 'monitoring'],
    max_active_runs=1,
) as dag:

    check_freshness = PythonOperator(
        task_id='check_data_freshness',
        python_callable=check_data_freshness,
    )

    check_completeness = PythonOperator(
        task_id='check_data_completeness',
        python_callable=check_data_completeness,
    )

    check_consistency = PythonOperator(
        task_id='check_data_consistency',
        python_callable=check_data_consistency,
    )

    notify_success = BashOperator(
        task_id='notify_success',
        bash_command='echo "[SUCCESS] DQ Check passed at $(date)"',
        trigger_rule='all_success',
    )

    [check_freshness, check_completeness, check_consistency] >> notify_success
