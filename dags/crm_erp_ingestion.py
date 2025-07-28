from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import bigquery
import subprocess
import logging

# Configurations
AIRBYTE_CONNECTION_ID = "your-airbyte-connection-id"
GCP_PROJECT = "coffee-analytics"
BQ_DATASET_RAW = "raw"
BQ_DATASET_ANALYTICS = "analytics"

def trigger_airbyte_sync(**kwargs):
    """
    Trigger Airbyte sync via CLI or API.
    """
    try:
        cmd = ["airbyte-api-client", "sync", AIRBYTE_CONNECTION_ID]
        result = subprocess.run(cmd, capture_output=True, text=True)
        logging.info(f"Airbyte Sync Output: {result.stdout}")
        if result.returncode != 0:
            raise Exception(f"Airbyte sync failed: {result.stderr}")
    except Exception as e:
        logging.error(f"Airbyte sync failed: {str(e)}")
        raise

def run_bigquery_transformations(**kwargs):
    """
    Run SQL transformations in BigQuery.
    """
    client = bigquery.Client(project=GCP_PROJECT)
    
    queries = [
        f"""
        CREATE OR REPLACE TABLE `{GCP_PROJECT}.{BQ_DATASET_ANALYTICS}.customers` AS
        SELECT
            Id AS customer_id,
            Name AS company_name,
            BillingCountry AS country,
            Industry AS customer_type,
            DATE(CreatedDate) AS start_date
        FROM `{GCP_PROJECT}.{BQ_DATASET_RAW}.salesforce_accounts`;
        """,
        f"""
        CREATE OR REPLACE TABLE `{GCP_PROJECT}.{BQ_DATASET_ANALYTICS}.orders`
        PARTITION BY DATE(order_date)
        CLUSTER BY customer_id AS
        SELECT
            Id AS order_id,
            AccountId AS customer_id,
            TotalAmount AS total_revenue,
            CloseDate AS order_date,
            StageName,
            Probability
        FROM `{GCP_PROJECT}.{BQ_DATASET_RAW}.salesforce_orders`
        WHERE StageName = 'Closed Won';
        """
    ]
    try:
        for query in queries:
            query_job = client.query(query)
            query_job.result()
            logging.info(f"Query succeeded: {query[:50]}...")
    except Exception as e:
        logging.error(f"BigQuery transformation failed: {str(e)}")
        raise

default_args = {
    'owner': 'you',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    'coffee_distributor_etl',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'salesforce', 'bigquery', 'airbyte'],
) as dag:

    task_airbyte_sync = PythonOperator(
        task_id='trigger_airbyte_sync',
        python_callable=trigger_airbyte_sync,
    )

    task_bigquery_transform = PythonOperator(
        task_id='run_bigquery_transformations',
        python_callable=run_bigquery_transformations,
    )

    task_airbyte_sync >> task_bigquery_transform
