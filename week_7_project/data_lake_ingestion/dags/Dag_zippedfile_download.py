from __future__ import print_function
import os
import logging

from datetime import datetime
import pytz

import kaggle
from kaggle.api.kaggle_api_extended import KaggleApi

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator

import csv
import requests
import json

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

api = KaggleApi()
api.authenticate()
api.dataset_download_files('lucabasa/dutch-energy')

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


ZIP_FILE_PATH = AIRFLOW_HOME + '/archive.zip'   # location save the zip operation to and read the unzip operator from

default_args = {
    "owner": "airflow",
    #"start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 0,
}

def download_zippedfile_dag(
    dag,
    url_template,
    path_to_zip_file,
   
):
    with dag:
        download_dataset_task = BashOperator(
            task_id="download_dataset_task",
            bash_command=f"curl -sSLf {url_template} > {path_to_zip_file}"
            #bash_command= kaggle datasets download {url_template} > {path_to_zip_file}"
        )
        
URL_PREFIX = 'https://www.kaggle.com/datasets/lucabasa/dutch-energy'
NETHERLANDS_ENERGY_URL_TEMPLATE = URL_PREFIX

archive_zippedfile_dag = DAG(
    dag_id="1st_dag_download_zipfile",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2011,1,1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['energy_project'],
)

download_zippedfile_dag(
    dag=archive_zippedfile_dag,
    url_template=NETHERLANDS_ENERGY_URL_TEMPLATE,
    path_to_zip_file=ZIP_FILE_PATH,
)