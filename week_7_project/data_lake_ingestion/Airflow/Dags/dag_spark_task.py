import airflow
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
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


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 4, 3),
    'retries': 10,
	  'retry_delay': timedelta(hours=1)
}
with airflow.DAG('4th_Dag_spark_job_task',
                  default_args=default_args,
                  schedule_interval='0 1 * * *') as dag:
                  spark_job_task = BashOperator(
                      task_id='spark_task',
                      bash_command="/usr/local/bin/python3.7 /opt/airflow/dags/tasks/dutch_energy_sparkscript.py")
