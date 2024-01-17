from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


GCP_PROJECT = 'learnde-4-1'
BQ_TARGET_DATASET = 'analytics'
BQ_TARGET_TABLE = 'daily_price_and_event_list'

sql_ctas_join = f"""
-- Drop the table if it exists
    DROP TABLE IF EXISTS {GCP_PROJECT}.{BQ_TARGET_DATASET}.{BQ_TARGET_TABLE};

    CREATE TABLE {GCP_PROJECT}.{BQ_TARGET_DATASET}.{BQ_TARGET_TABLE}
    AS
    WITH
    event AS (
        SELECT
            event_date AS date,
            event_name,
            CASE
                WHEN event_name = '졸업식' THEN event_count * 5
                ELSE event_count * 5000
            END AS modified_event_count
        FROM
            learnde-4-1.flower_prj_schema.event_list
        WHERE
            (event_name != '졸업식' OR (event_name = '졸업식' AND event_count >= 100))
            AND event_date < '2024-01-14'
    ),
    flower AS (
        SELECT
            saleDate AS date,
            SUM(totAmt) AS totAmt,  -- Assuming you want to sum totAmt, adjust this accordingly
            totQty
        FROM
            learnde-4-1.raw_data.flower_external
        GROUP BY date, totQty  -- Adjust the GROUP BY clause accordingly
    )
    SELECT
        COALESCE(flower.date, event.date) AS date,
        flower.totAmt, 
        flower.totQty,
        event.event_name,
        event.modified_event_count
    FROM
        flower
    FULL JOIN
        event ON flower.date = event.date
"""


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 1),
    'catchup': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    'create_daily_price_and_event',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
) as dag:

    ctas_with_join_for_event = BigQueryInsertJobOperator(
        task_id="ctas_with_join_for_event",
        gcp_conn_id='gcp_conn',
        configuration={
            "query": {
                "query": sql_ctas_join,
                "useLegacySql": False,
            }
        }
    )

    ctas_with_join_for_event

