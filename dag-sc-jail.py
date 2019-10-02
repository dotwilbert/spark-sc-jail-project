#! /usr/bin/env python

import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

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

dag = DAG('tutorial',
          default_args=default_args,
          schedule_interval='0 7 * * *',
          dagrun_timeout=timedelta(minutes=5),
          )

dl_task = BashOperator(
    task_id='dl_population_sheet',
    bash_command='source /home/airflow/.bashrc; conda activate sc-jail-project; /home/airflow/scripts/sc-jail-project/dl-santa-clara-dpcs.py -o /bigdata/{{ ds_nodash }}-santa-clara-daily-population-sheet.pdf',
    params={'retries': 3},
    dag=dag,
)

sheet_to_text_task = BashOperator(
    task_id='sheet_to_text',
    bash_command='source /home/airflow/.bashrc; conda activate sc-jail-project; /home/airflow/scripts/sc-jail-project/convert-dpcs-to-text.py \
    -i /bigdata/{{ ds_nodash }}-santa-clara-daily-population-sheet.pdf \
    -o /bigdata/{{ ds_nodash }}-santa-clara-daily-population-sheet.txt \
    --keep-infile \
    --keep-imagefile',
    dag=dag
)

dl_task >> sheet_to_text_task

if __name__ == "__main__":
    dag.cli()
