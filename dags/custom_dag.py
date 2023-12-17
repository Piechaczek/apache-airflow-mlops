import datetime
import json
import os

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task

@task(task_id="download the data")
def download_kaggle_data_task(kaggle_json):
    os.environ["KAGGLE_USERNAME"] = kaggle_json['username']
    os.environ["KAGGLE_KEY"] = kaggle_json['key']

    # TODO this is a workaround for dependencies
    import sys
    import subprocess

    subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'kaggle'])

    # TODO replace kaggle with S3
    import kaggle

    kaggle.api.authenticate()
    kaggle.api.dataset_download_files('rtatman/digidb', path='/usr/local/tmp/data/', unzip=True)


with DAG(
    dag_id="my_dag_name",
    start_date=datetime.datetime(2021, 1, 1),
    schedule="@daily",
):
    kaggle_json = None
    with open('./kaggle.json') as f:
        kaggle_json = json.load(f)

    download_task = download_kaggle_data_task(kaggle_json)
    dummy_task = EmptyOperator(task_id="Dummy task")
    download_task >> dummy_task
