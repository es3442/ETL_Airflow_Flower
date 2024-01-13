from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


GCP_PROJECT = 'learnde-4-1'
BQ_SOURCE_DATASET = 'raw_data'
BQ_SOURCE_TABLE = 'weather_external'
BQ_TARGET_DATASET = 'analytics'
BQ_TARGET_TABLE = 'daily_avg_temp'


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 1),
    'catchup': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

sql_ctas = f"""
    CREATE OR REPLACE TABLE { GCP_PROJECT }.{ BQ_TARGET_DATASET }.{ BQ_TARGET_TABLE }
    AS
    SELECT
        tm AS date,
        avgTa AS avg_temp
    FROM { GCP_PROJECT }.{ BQ_SOURCE_DATASET }.{ BQ_SOURCE_TABLE }
"""

with DAG(
    'weather_build_summary',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
) as dag:

    ctas = BigQueryInsertJobOperator(
        task_id="ctas",
        gcp_conn_id='gcp_conn',
        configuration={
            "query": {
                "query": sql_ctas,
                "useLegacySql": False,
            }
        }
    )

    ctas
