from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email
import logging
import json

# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering',
   'retries': 2,
   'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
}

# Initialize the DAG
dag = DAG(
    dag_id='sigma_transaction_pipeline',
    schedule='0 2 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    sla_miss_callback=lambda context: send_email(
        to=["alerts@sigmadatatech.com"],
        subject=f"SLA Miss - DAG: {context['dag'].dag_id}, Execution Date: {context['execution_date']}",
        html_content=f"<p>DAG {context['dag'].dag_id} missed its SLA for execution date {context['execution_date']}</p>"
    ),
    tags=["sigma", "transactions", "daily"],
    description="Daily Bronze->Silver->Gold pipeline for Sigma DataTech transactions",
)

def on_failure_callback(context):
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    execution_date = context['execution_date']
    error_message = context['exception']
    logging.error(f"DAG: {dag_id}, Task: {task_id}, Execution Date: {execution_date}, Error: {error_message}")

def extract_bronze(**context):
    """Ingest raw CSVs to Bronze Parquet"""
    logging.info("Starting extract_bronze task")
    # Placeholder for actual logic
    logging.info("Ending extract_bronze task")
    raise Exception("Simulated failure")  # Remove in production

def transform_silver(**context):
    """Clean, enrich, deduplicate to Silver"""
    logging.info("Starting transform_silver task")
    # Placeholder for actual logic
    logging.info("Ending transform_silver task")
    raise Exception("Simulated failure")  # Remove in production

def build_gold(**context):
    """Generate the 3 Gold aggregation tables"""
    logging.info("Starting build_gold task")
    # Placeholder for actual logic
    logging.info("Ending build_gold task")
    raise Exception("Simulated failure")  # Remove in production

# Define tasks with dependencies and on_failure_callback
extract_bronze_task = PythonOperator(
    task_id='extract_bronze',
    python_callable=extract_bronze,
    on_failure_callback=on_failure_callback,
    dag=dag,
)

transform_silver_task = PythonOperator(
    task_id='transform_silver',
    python_callable=transform_silver,
    on_failure_callback=on_failure_callback,
    dag=dag,
)

build_gold_task = PythonOperator(
    task_id='build_gold',
    python_callable=build_gold,
    on_failure_callback=on_failure_callback,
    dag=dag,
)

# Set task dependencies
extract_bronze_task >> transform_silver_task >> build_gold_task
