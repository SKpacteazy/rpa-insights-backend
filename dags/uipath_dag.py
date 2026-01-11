from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Ensure uipath_etl is importable
# In the docker-compose setup, we mounted ./uipath_etl to /opt/airflow/uipath_etl
# /opt/airflow is usually in PYTHONPATH, so uipath_etl should be importable as a package.
try:
    from uipath_etl.client import UiPathClient
except ImportError:
    # Fallback if not in path (e.g. testing locally)
    sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
    from uipath_etl.client import UiPathClient

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'uipath_etl_pipeline',
    default_args=default_args,
    description='A specific DAG for UiPath ETL',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

def run_etl():
    client = UiPathClient()
    result = client.run_etl()
    print("ETL Job Finished. Result summary:", len(result.get('value', [])) if result else "No Data")

def run_update_job():
    client = UiPathClient()
    result = client.run_update_job()
    print("Update Job Finished. Result summary:", len(result.get('value', [])) if result else "No Data")

def run_jobs_etl():
    client = UiPathClient()
    result = client.run_jobs_etl()
    print("Jobs ETL Finished. Result summary:", len(result.get('value', [])) if result else "No Data")

with dag:
    run_etl_task = PythonOperator(
        task_id='run_uipath_etl',
        python_callable=run_etl,
    )

    run_update_task = PythonOperator(
        task_id='update_queue_items',
        python_callable=run_update_job,
    )

    run_jobs_task = PythonOperator(
        task_id='run_jobs_etl',
        python_callable=run_jobs_etl,
    )

    # You can set dependencies here if needed, e.g., run_etl_task >> run_update_task
    # Or keep them independent as per request "add below api to update" implies a separate action.
    # For now, I'll list them both so they appear in the DAG.
    run_etl_task
    run_update_task
    run_jobs_task
