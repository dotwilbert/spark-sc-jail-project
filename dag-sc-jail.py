#! /usr/bin/env python

import uuid
from datetime import datetime, timedelta

import airflow
import pendulum
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': '##TODAY##',
    'email': ['airflow@theairflower.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

# Every day schedule will run at +2/+3hrs after publication. Container in UTC, publication in Pacific
dag = DAG('sc-jail-daily-population-count-sheet',
          default_args=default_args,
          schedule_interval='0 14 * * *',
          user_defined_filters={
              'iso8601': lambda t: t.to_iso8601_string(),
              'to_pacific_date': lambda utc_iso8601: pendulum.parse(utc_iso8601)
              .in_timezone('America/Los_Angeles')
              .format('YYYYMMDD', formatter='alternative')
          },
          dagrun_timeout=timedelta(minutes=5),
          )


def generate_load_id(**context):
    context['ti'].xcom_push(key='load_id', value=str(uuid.uuid4()))


generate_load_id_task = PythonOperator(
    task_id='generate_load_id',
    dag=dag,
    python_callable=generate_load_id,
    provide_context=True
)

dl_task = BashOperator(
    task_id='dl_population_sheet',
    bash_command='source /home/airflow/.conda_environment;\
        conda activate airflow-project;\
        /home/airflow/scripts/sc-jail-project/dl-santa-clara-dpcs.py\
        -o /bigdata/{{ execution_date | iso8601 | to_pacific_date }}-santa-clara-daily-population-sheet.pdf',
    params={'retries': 3},
    dag=dag,
)

sheet_to_text_task = BashOperator(
    task_id='sheet_to_text',
    bash_command='source /home/airflow/.conda_environment; conda activate airflow-project; \
    /home/airflow/scripts/sc-jail-project/convert-dpcs-to-text.py \
    -i /bigdata/{{ execution_date | iso8601 | to_pacific_date }}-santa-clara-daily-population-sheet.pdf \
    -o /bigdata/{{ execution_date | iso8601 | to_pacific_date }}-santa-clara-daily-population-sheet.txt \
    --keep-infile',
    dag=dag,
)

load_with_spark_task = BashOperator(
    task_id='load_with_spark',
    bash_command="source /home/airflow/.conda_environment; conda activate airflow-project; \
    /opt/spark-2.4.4-bin-hadoop2.7/bin/spark-submit \
    --master spark://sparkmaster:7077 \
    --driver-class-path /usr/share/java/postgresql.jar \
    --jars local:///usr/share/java/postgresql.jar \
    /home/airflow/scripts/sc-jail-project/load-dpcs.py \
    -g '/bigdata/{{ execution_date | iso8601 | to_pacific_date }}-santa-clara-daily-population-sheet.txt' \
    -u $SC_JAIL_USER \
    -p $SC_JAIL_PASSWORD \
    -s $SC_JAIL_DB \
    -i {{ task_instance.xcom_pull(key='load_id', task_ids='generate_load_id')}} \
    --archive-infile",
    dag=dag,
)

staging_to_tables_task = BashOperator(
    task_id='staging_to_tables',
    bash_command="/home/airflow/bin/staging2prod \
        -c /home/airflow/bin/staging2prod.properties \
        -l {{ task_instance.xcom_pull(key='load_id', task_ids='generate_load_id')}}",
    dag=dag,
)

only_run_current_date_task = LatestOnlyOperator (
    task_id='only_run_current_date',
    dag=dag,
)

only_run_current_date_task >> dl_task >> sheet_to_text_task >> load_with_spark_task
only_run_current_date_task >> generate_load_id_task >> load_with_spark_task
load_with_spark_task >> staging_to_tables_task

if __name__ == "__main__":
    dag.cli()
