import datetime
import json
import os

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task

kaggle_json = None

@task(task_id="download the data")
def download_kaggle_data_task():
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


def main():
    global kaggle_json
    with open('./kaggle.json') as f:
        kaggle_json = json.load(f)

    with DAG(
        dag_id="my_dag_name",
        start_date=datetime.datetime(2021, 1, 1),
        schedule="@daily",
    ):
        download_task = download_kaggle_data_task()


if '__name__' == '__main__':
    main()