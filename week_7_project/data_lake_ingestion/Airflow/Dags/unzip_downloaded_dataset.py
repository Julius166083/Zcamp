#  dag_kaggle_download.py

import os

from datetime import datetime
from datetime import datetime, timedelta

import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.operators.python_operator import PythonOperator

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

import csv
import requests
import json
import csv
import requests
import json
import zipfile
import requests
from zipfile import ZipFile
import shutil



# Unzip downloaded dataset
def unzip_data():
    """
    Load downloaded data and unzip in newly created directory
    """
    zip_path = os.getcwd()+"/dutch-energy.zip"
    # create folder called energy where files will be unziped
    extract_file = os.path.join("datasets", "energy")
    print(extract_file)
    # check if path directory already exist
    if os.path.exists(extract_file):
        shutil.rmtree('./datasets')
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_file)

unzip_data()


#========================================

default_args = {
    'owner': 'airflow',
    "depends_on_past": False,
    'retries': 2,
	 # 'retry_delay': timedelta(hours=1)
}
with airflow.DAG('2nd_Dag_unzip_downloaded_dataset',
                  default_args=default_args,
                  schedule_interval="0 6 2 * *",
                  start_date=datetime(2022,4,3),
                  end_date=datetime(2022,4,28)) as dag:
                  unzip_task = PythonOperator(
                      task_id='unzip_task',
                      python_callable=unzip_data)
