#! /usr/bin/env python

from datetime import datetime, timedelta

import pytz

import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
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
          user_defined_filters= {
              'iso8601': lambda t: t.format('YYYY-MM-DDTHH:mm:ssZ', formatter='alternative'),
              'to_pacific_tz': lambda utc_iso8601: datetime.strptime(utc_iso8601, "%Y-%m-%dT%H:%M:%S%z") \
                  .astimezone(pytz.timezone('America/Los_Angeles')) \
                  .strftime('%Y%m%d') 
          },
          dagrun_timeout=timedelta(minutes=5),
          )

dl_task = BashOperator(
    task_id='dl_population_sheet',
    bash_command='source /home/airflow/.conda_environment; conda activate sc-jail-project; /home/airflow/scripts/sc-jail-project/dl-santa-clara-dpcs.py -o /bigdata/{{ execution_date | iso8601 | to_pacific_tz }}-santa-clara-daily-population-sheet.pdf',
    params={'retries': 3},
    dag=dag,
)

sheet_to_text_task = BashOperator(
    task_id='sheet_to_text',
    bash_command='source /home/airflow/.conda_environment; conda activate sc-jail-project; \
    /home/airflow/scripts/sc-jail-project/convert-dpcs-to-text.py \
    -i /bigdata/{{ execution_date | iso8601 | to_pacific_tz }}-santa-clara-daily-population-sheet.pdf \
    -o /bigdata/{{ execution_date | iso8601 | to_pacific_tz }}-santa-clara-daily-population-sheet.txt \
    --keep-infile \
    --keep-imagefile',
    dag=dag
)

dl_task >> sheet_to_text_task

load_with_spark_task = BashOperator(
    task_id='load_with_spark',
    bash_command="source /home/airflow/.conda_environment; conda activate sc-jail-project; \
    /opt/spark-2.4.4-bin-hadoop2.7/bin/spark-submit \
    --master spark://sparkmaster:7077 \
    --driver-class-path /usr/share/java/postgresql.jar \
    --jars local:///usr/share/java/postgresql.jar \
    /home/airflow/scripts/sc-jail-project/load-dpcs.py \
    -g '/bigdata/*-santa-clara-daily-population-sheet.txt' \
    -u $SC_JAIL_USER \
    -p $SC_JAIL_PASSWORD \
    -s $SC_JAIL_DB \
    --archive-infile",
    dag=dag
)

dl_task >> sheet_to_text_task >> load_with_spark_task

if __name__ == "__main__":
    dag.cli()
