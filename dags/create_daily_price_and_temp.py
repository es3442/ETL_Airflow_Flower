from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


GCP_PROJECT = 'learnde-4-1'
BQ_TARGET_DATASET = 'analytics'
BQ_TARGET_TABLE = 'daily_price_and_temp'

sql_ctas_join = f"""
    CREATE OR REPLACE TABLE { GCP_PROJECT }.{ BQ_TARGET_DATASET }.{ BQ_TARGET_TABLE } 
    AS
    WITH
    temp AS (
        SELECT
            tm AS date,
            avgTa AS avg_temperature
        FROM
            learnde-4-1.raw_data.weather_external
    ),
    flower AS (
        SELECT
            saleDate AS date,
            SUM(totAmt) / SUM(totQty) AS avg_price_per_quantity
        FROM
            learnde-4-1.raw_data.flower_external
        GROUP BY 1
    )
    SELECT
        temp.date,
        temp.avg_temperature,
        flower.avg_price_per_quantity
    FROM
        temp
    JOIN
        flower ON temp.date = flower.date;
"""


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 1),
    'catchup': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    'create_daily_price_and_temp',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
) as dag:

    ctas_with_join = BigQueryInsertJobOperator(
        task_id="ctas_with_join",
        gcp_conn_id='gcp_conn',
        configuration={
            "query": {
                "query": sql_ctas_join,
                "useLegacySql": False,
            }
        }
    )

    ctas_with_join
