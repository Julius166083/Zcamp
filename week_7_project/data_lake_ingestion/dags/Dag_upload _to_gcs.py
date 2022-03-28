from __future__ import print_function
import os
import logging

from datetime import datetime
from datetime import datetime, timedelta
import pytz

# imports from airflow module
from airflow import DAG
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from zipfile import ZipFile

# importing zip_operator_plugin operators
from zip_operator_plugin import ZipOperatorPlugin
from zip_operator_plugin import UnzipOperator

import csv
import requests
import json

from google.cloud import storage

import pyarrow.csv as pv
import pyarrow.parquet as pq


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


def format_to_parquet(src_file, dest_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, dest_file)


def upload_to_gcs(bucket, object_name, local_file):
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    #"start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 0,
}


def download_parquetize_upload_dag(
    dag,
    local_csv_path_template,
    local_parquet_path_template,
    gcs_path_template
):
    with dag:
        format_to_parquet_task = PythonOperator(
            task_id="format_to_parquet_task",
            python_callable=format_to_parquet,
            op_kwargs={
                "src_file": local_csv_path_template,
                "dest_file": local_parquet_path_template
            },
        )

        local_to_gcs_task = PythonOperator(
            task_id="local_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": gcs_path_template,
                "local_file": local_parquet_path_template,
            },
        )

        rm_task = BashOperator(
            task_id="rm_task",
            bash_command=f"rm {local_csv_path_template} {local_parquet_path_template}"
        )
    
        format_to_parquet_task >> local_to_gcs_task >> rm_task


################################################################################
################# NETHERLAND ELECTRICAL ENERGY FILES############################
################################################################################

## COTEQ_ELECTRICITY_FILES

COTEQ_ELECTRICITY_CSV_FILE_TEMPLATE = AIRFLOW_HOME + '/Electricity/coteq_electricity_{{ execution_date.strftime(\'%Y\') }}.csv'
COTEQ_ELECTRICITY_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + '/Electricity/coteq_electricity_{{ execution_date.strftime(\'%Y\') }}.parquet'
#COTEQ_ELECTRICITY_GCS_PATH_TEMPLATE = "ELECTRICITY_COTEQ/coteq_electricity/{{ execution_date.strftime(\'%Y\') }}/coteq_electricity_{{ execution_date.strftime(\'%Y\') }}.parquet"
COTEQ_ELECTRICITY_GCS_PATH_TEMPLATE = "ELECTRICITY_COTEQ/coteq_electricity_{{ execution_date.strftime(\'%Y\') }}.parquet"

coteq_electricity_dag = DAG(
    dag_id="dag_coteq_electricity",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2013,1,1),
    end_date=datetime(2021,1,1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['energy-project'],
)

download_parquetize_upload_dag(
    dag=coteq_electricity_dag,
    local_csv_path_template=COTEQ_ELECTRICITY_CSV_FILE_TEMPLATE,
    local_parquet_path_template=COTEQ_ELECTRICITY_PARQUET_FILE_TEMPLATE,
    gcs_path_template=COTEQ_ELECTRICITY_GCS_PATH_TEMPLATE
)


## RENDO_ELECTRICITY_FILES

RENDO_ELECTRICITY_CSV_FILE_TEMPLATE = AIRFLOW_HOME + '/Electricity/rendo_electricity_{{ execution_date.strftime(\'%Y\') }}.csv'
RENDO_ELECTRICITY_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + '/Electricity/rendo_electricity_{{ execution_date.strftime(\'%Y\') }}.parquet'
RENDO_ELECTRICITY_GCS_PATH_TEMPLATE = "ELECTRICITY_RENDO/rendo_electricity_{{ execution_date.strftime(\'%Y\') }}.parquet"

rendo_electricity_dag = DAG(
    dag_id="dag_rendo_electricity",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2011,1,1),
    end_date=datetime(2020,1,1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['energy-project'],
)

download_parquetize_upload_dag(
    dag=rendo_electricity_dag,
    local_csv_path_template=RENDO_ELECTRICITY_CSV_FILE_TEMPLATE,
    local_parquet_path_template=RENDO_ELECTRICITY_PARQUET_FILE_TEMPLATE,
    gcs_path_template=RENDO_ELECTRICITY_GCS_PATH_TEMPLATE
)

## STEDIN_ELECTRICITY_FILES

STEDIN_ELECTRICITY_CSV_FILE_TEMPLATE = AIRFLOW_HOME + '/Electricity/stedin_electricity_{{ execution_date.strftime(\'%Y\') }}.csv'
STEDIN_ELECTRICITY_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + '/Electricity/stedin_electricity_{{ execution_date.strftime(\'%Y\') }}.parquet'
STEDIN_ELECTRICITY_GCS_PATH_TEMPLATE = "ELECTRICITY_STEDIN/stedin_electricity_{{ execution_date.strftime(\'%Y\') }}.parquet"

stedin_electricity_dag = DAG(
    dag_id="dag_stedin_electricity",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2009,1,1),
    end_date=datetime(2021,1,1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['energy-project'],
)

download_parquetize_upload_dag(
    dag=stedin_electricity_dag,
    local_csv_path_template=STEDIN_ELECTRICITY_CSV_FILE_TEMPLATE,
    local_parquet_path_template=STEDIN_ELECTRICITY_PARQUET_FILE_TEMPLATE,
    gcs_path_template=STEDIN_ELECTRICITY_GCS_PATH_TEMPLATE
)

## WESTLAND-INFRA_ELECTRICITY_FILES

#'WESTLAND-INFRA_ELECTRICITY_CSV_FILE_TEMPLATE' = AIRFLOW_HOME + '/Electricity/westland-infra_electricity_{{ execution_date.strftime(\'%Y\') }}.csv'
#'WESTLAND-INFRA_ELECTRICITY_PARQUET_FILE_TEMPLATE' = AIRFLOW_HOME + '/Electricity/westland-infra_electricity_{{ execution_date.strftime(\'%Y\') }}.parquet'
#'WESTLAND-INFRA_ELECTRICITY_GCS_PATH_TEMPLATE' = "ELECTRICITY_WESTLAND-INFRA/westland-infra_electricity_{{ execution_date.strftime(\'%Y\') }}.parquet"
#
#westland-infra_electricity_dag = DAG(
#    dag_id="dag_westland-infra_electricity",
#    schedule_interval="0 6 2 * *",
#    start_date=datetime(2011,1,1),
#    end_date=datetime(2021,1,1),
#    default_args=default_args,
#    catchup=True,
#    max_active_runs=3,
#    tags=['energy-project'],
#)
#
#download_parquetize_upload_dag(
#    dag=westland-infra_electricity_dag,
#    local_csv_path_template='WESTLAND-INFRA_ELECTRICITY_CSV_FILE_TEMPLATE',
#    local_parquet_path_template='WESTLAND-INFRA_ELECTRICITY_PARQUET_FILE_TEMPLATE',
#    gcs_path_template='WESTLAND-INFRA_ELECTRICITY_GCS_PATH_TEMPLATE
#)'
## ENDINET_ELECTRICITY_FILES

ENDINET_ELECTRICITY_CSV_FILE_TEMPLATE = AIRFLOW_HOME + '/Electricity/endinet_electricity_{{ execution_date.strftime(\'0101%Y\') }}.csv'
ENDINET_ELECTRICITY_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + '/Electricity/endinet_electricity_{{ execution_date.strftime(\'0101%Y\') }}.parquet'
ENDINET_ELECTRICITY_GCS_PATH_TEMPLATE = "ELECTRICITY_ENDINET/endinet_electricity_{{ execution_date.strftime(\'0101%Y\') }}.parquet"

endinet_electricity_dag = DAG(
    dag_id="dag_endinet_electricity",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2011,1,1),
    end_date=datetime(2017,1,1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['energy-project'],
)

download_parquetize_upload_dag(
    dag=endinet_electricity_dag,
    local_csv_path_template=ENDINET_ELECTRICITY_CSV_FILE_TEMPLATE,
    local_parquet_path_template=ENDINET_ELECTRICITY_PARQUET_FILE_TEMPLATE,
    gcs_path_template=ENDINET_ELECTRICITY_GCS_PATH_TEMPLATE
)

## ENDURIS_ELECTRICITY_FILES

ENDURIS_ELECTRICITY_CSV_FILE_TEMPLATE = AIRFLOW_HOME + '/Electricity/enduriselectricity_{{ execution_date.strftime(\'0101%Y\') }}.csv'
ENDURIS_ELECTRICITY_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + '/Electricity/enduriselectricity_{{ execution_date.strftime(\'0101%Y\') }}.parquet'
ENDURIS_ELECTRICITY_GCS_PATH_TEMPLATE = "ELECTRICITY_ENDURIS/enduriselectricity_{{ execution_date.strftime(\'0101%Y\') }}.parquet"

enduris_electricity_dag = DAG(
    dag_id="dag_enduris_electricity",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2013,1,1),
    end_date=datetime(2020,1,1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['energy-project'],
)

download_parquetize_upload_dag(
    dag=enduris_electricity_dag,
    local_csv_path_template=ENDURIS_ELECTRICITY_CSV_FILE_TEMPLATE,
    local_parquet_path_template=ENDURIS_ELECTRICITY_PARQUET_FILE_TEMPLATE,
    gcs_path_template=ENDURIS_ELECTRICITY_GCS_PATH_TEMPLATE
)

## ENEXIS_ELECTRICITY_FILES

ENEXIS_ELECTRICITY_CSV_FILE_TEMPLATE = AIRFLOW_HOME + '/Electricity/enexis_electricity_{{ execution_date.strftime(\'0101%Y\') }}.csv'
ENEXIS_ELECTRICITY_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + '/Electricity/enexis_electricity_{{ execution_date.strftime(\'0101%Y\') }}.parquet'
ENEXIS_ELECTRICITY_GCS_PATH_TEMPLATE = "ELECTRICITY_ENEXIS/enexis_electricity_{{ execution_date.strftime(\'0101%Y\') }}.parquet"

enexis_electricity_dag = DAG(
    dag_id="dag_enexis_electricity",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2010,1,1),
    end_date=datetime(2021,1,1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['energy-project'],
)

download_parquetize_upload_dag(
    dag=enexis_electricity_dag,
    local_csv_path_template=ENEXIS_ELECTRICITY_CSV_FILE_TEMPLATE,
    local_parquet_path_template=ENEXIS_ELECTRICITY_PARQUET_FILE_TEMPLATE,
    gcs_path_template=ENEXIS_ELECTRICITY_GCS_PATH_TEMPLATE
)

## LIANDER_ELECTRICITY_FILES

LIANDER_ELECTRICITY_CSV_FILE_TEMPLATE = AIRFLOW_HOME + '/Electricity/liander_electricity_{{ execution_date.strftime(\'0101%Y\') }}.csv'
LIANDER_ELECTRICITY_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + '/Electricity/liander_electricity_{{ execution_date.strftime(\'0101%Y\') }}.parquet'
LIANDER_ELECTRICITY_GCS_PATH_TEMPLATE = "ELECTRICITY_LIANDER/liander_electricity_{{ execution_date.strftime(\'0101%Y\') }}.parquet"

liander_electricity_dag = DAG(
    dag_id="dag_liander_electricity",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2009,1,1),
    end_date=datetime(2021,1,1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['energy-project'],
)

download_parquetize_upload_dag(
    dag=liander_electricity_dag,
    local_csv_path_template=LIANDER_ELECTRICITY_CSV_FILE_TEMPLATE,
    local_parquet_path_template=LIANDER_ELECTRICITY_PARQUET_FILE_TEMPLATE,
    gcs_path_template=LIANDER_ELECTRICITY_GCS_PATH_TEMPLATE
)



################################################################################
################# NETHERLAND GAS ENERGY FILES #################################
################################################################################

## COTEQ_GAS_ENERGY_FILES

COTEQ_GAS_CSV_FILE_TEMPLATE = AIRFLOW_HOME + '/Gas/coteq_gas_{{ execution_date.strftime(\'%Y\') }}.csv'
COTEQ_GAS_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + '/Gas/coteq_gas_{{ execution_date.strftime(\'%Y\') }}.parquet'
COTEQ_GAS_GCS_PATH_TEMPLATE = "GAS_COTEQ/coteq_gas_{{ execution_date.strftime(\'%Y\') }}.parquet"


coteq_gas_dag = DAG(
    dag_id="dag_coteq_gas",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2013,1,1),
    end_date=datetime(2021,1,1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['energy-project'],
)

download_parquetize_upload_dag(
    dag=coteq_gas_dag,
    local_csv_path_template=COTEQ_GAS_CSV_FILE_TEMPLATE,
    local_parquet_path_template=COTEQ_GAS_PARQUET_FILE_TEMPLATE,
    gcs_path_template=COTEQ_GAS_GCS_PATH_TEMPLATE
)


## RENDO_GAS_ENERGY_FILES

RENDO_GAS_CSV_FILE_TEMPLATE = AIRFLOW_HOME + '/Gas/rendo_gas_{{ execution_date.strftime(\'%Y\') }}.csv'
RENDO_GAS_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + '/Gas/rendo_gas_{{ execution_date.strftime(\'%Y\') }}.parquet'
RENDO_GAS_GCS_PATH_TEMPLATE = "GAS_RENDO/rendo_gas_{{ execution_date.strftime(\'%Y\') }}.parquet"

rendo_gas_dag = DAG(
    dag_id="dag_rendo_gas",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2011,1,1),
    end_date=datetime(2020,1,1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['energy-project'],
)

download_parquetize_upload_dag(
    dag=rendo_gas_dag,
    local_csv_path_template=RENDO_GAS_CSV_FILE_TEMPLATE,
    local_parquet_path_template=RENDO_GAS_PARQUET_FILE_TEMPLATE,
    gcs_path_template=RENDO_GAS_GCS_PATH_TEMPLATE
)

## STEDIN_GAS_ENERGY_FILES

STEDIN_GAS_CSV_FILE_TEMPLATE = AIRFLOW_HOME + '/Gas/stedin_gas_{{ execution_date.strftime(\'%Y\') }}.csv'
STEDIN_GAS_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + '/Gas/stedin_gas_{{ execution_date.strftime(\'%Y\') }}.parquet'
STEDIN_GAS_GCS_PATH_TEMPLATE = "GAS_STEDIN/stedin_gas_{{ execution_date.strftime(\'%Y\') }}.parquet"

stedin_gas_dag = DAG(
    dag_id="dag_stedin_gas",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2009,1,1),
    end_date=datetime(2021,1,1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['energy-project'],
)

download_parquetize_upload_dag(
    dag=stedin_gas_dag,
    local_csv_path_template=STEDIN_GAS_CSV_FILE_TEMPLATE,
    local_parquet_path_template=STEDIN_GAS_PARQUET_FILE_TEMPLATE,
    gcs_path_template=STEDIN_GAS_GCS_PATH_TEMPLATE
)

## WESTLAND-INFRA_GAS_ENERGY_FILES

#WESTLAND-INFRA_GAS_CSV_FILE_TEMPLATE = AIRFLOW_HOME + '/Gas/westland-infra_gas_{{ execution_date.strftime(\'%Y\') }}.csv'
#WESTLAND-INFRA_GAS_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + '/Gas/westland-infra_gas_{{ execution_date.strftime(\'%Y\') }}.parquet'
#WESTLAND-INFRA_GAS_GCS_PATH_TEMPLATE = "GAS_WESTLAND-INFRA/westland-infra_gas_{{ execution_date.strftime(\'%Y\') }}.parquet"
#
#westland-infra_gas_dag = DAG(
#    dag_id="dag_westland-infra_gas",
#    schedule_interval="0 6 2 * *",
#    start_date=datetime(2011,1,1),
#    end_date=datetime(2021,1,1),
#    default_args=default_args,
#    catchup=True,
#    max_active_runs=3,
#    tags=['energy-project'],
#)
#
#download_parquetize_upload_dag(
#    dag=westland-infra_gas_dag,
#    local_csv_path_template=WESTLAND-INFRA_GAS_CSV_FILE_TEMPLATE,
#    local_parquet_path_template=WESTLAND-INFRA_GAS_PARQUET_FILE_TEMPLATE,
#    gcs_path_template=WESTLAND-INFRA_GAS_GCS_PATH_TEMPLATE
#)

## ENDINET_GAS_ENERGY_FILES

ENDINET_GAS_CSV_FILE_TEMPLATE = AIRFLOW_HOME + '/Gas/endinet_gas_{{ execution_date.strftime(\'0101%Y\') }}.csv'
ENDINET_GAS_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + '/Gas/endinet_gas_{{ execution_date.strftime(\'0101%Y\') }}.parquet'
ENDINET_GAS_GCS_PATH_TEMPLATE = "GAS_ENDINET/endinet_gas_{{ execution_date.strftime(\'0101%Y\') }}.parquet"

endinet_gas_dag = DAG(
    dag_id="dag_endinet_gas",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2011,1,1),
    end_date=datetime(2017,1,1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['energy-project'],
)

download_parquetize_upload_dag(
    dag=endinet_gas_dag,
    local_csv_path_template=ENDINET_GAS_CSV_FILE_TEMPLATE,
    local_parquet_path_template=ENDINET_GAS_PARQUET_FILE_TEMPLATE,
    gcs_path_template=ENDINET_GAS_GCS_PATH_TEMPLATE
)

## ENDURIS_GAS_ENERGY_FILES

ENDURIS_GAS_CSV_FILE_TEMPLATE = AIRFLOW_HOME + '/Gas/endurisgas_{{ execution_date.strftime(\'0101%Y\') }}.csv'
ENDURIS_GAS_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + '/Gas/endurisgas_{{ execution_date.strftime(\'0101%Y\') }}.parquet'
ENDURIS_GAS_GCS_PATH_TEMPLATE = "GAS_ENDURIS/endurisgas_{{ execution_date.strftime(\'0101%Y\') }}.parquet"

enduris_gas_dag = DAG(
    dag_id="dag_enduris_gas",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2013,1,1),
    end_date=datetime(2020,1,1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['energy-project'],
)

download_parquetize_upload_dag(
    dag=enduris_gas_dag,
    local_csv_path_template=ENDURIS_GAS_CSV_FILE_TEMPLATE,
    local_parquet_path_template=ENDURIS_GAS_PARQUET_FILE_TEMPLATE,
    gcs_path_template=ENDURIS_GAS_GCS_PATH_TEMPLATE
)

## ENEXIS_GAS_ENERGY_FILES

ENEXIS_GAS_CSV_FILE_TEMPLATE = AIRFLOW_HOME + '/Gas/enexis_gas_{{ execution_date.strftime(\'0101%Y\') }}.csv'
ENEXIS_GAS_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + '/Gas/enexis_gas_{{ execution_date.strftime(\'0101%Y\') }}.parquet'
ENEXIS_GAS_GCS_PATH_TEMPLATE = "GAS_ENEXIS/enexis_gas_{{ execution_date.strftime(\'0101%Y\') }}.parquet"

enexis_gas_dag = DAG(
    dag_id="dag_enexis_gas",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2010,1,1),
    end_date=datetime(2021,1,1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['energy-project'],
)

download_parquetize_upload_dag(
    dag=enexis_gas_dag,
    local_csv_path_template=ENEXIS_GAS_CSV_FILE_TEMPLATE,
    local_parquet_path_template=ENEXIS_GAS_PARQUET_FILE_TEMPLATE,
    gcs_path_template=ENEXIS_GAS_GCS_PATH_TEMPLATE
)

## LIANDER_GAS_ENERGY_FILES

LIANDER_GAS_CSV_FILE_TEMPLATE = AIRFLOW_HOME + '/Gas/liander_gas_{{ execution_date.strftime(\'0101%Y\') }}.csv'
LIANDER_GAS_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + '/Gas/liander_gas_{{ execution_date.strftime(\'0101%Y\') }}.parquet'
LIANDER_GAS_GCS_PATH_TEMPLATE = "GAS_LIANDER/liander_gas_{{ execution_date.strftime(\'0101%Y\') }}.parquet"

liander_gas_dag = DAG(
    dag_id="dag_liander_gas",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2009,1,1),
    end_date=datetime(2021,1,1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['energy-project'],
)

download_parquetize_upload_dag(
    dag=liander_gas_dag,
    local_csv_path_template=LIANDER_GAS_CSV_FILE_TEMPLATE,
    local_parquet_path_template=LIANDER_GAS_PARQUET_FILE_TEMPLATE,
    gcs_path_template=LIANDER_GAS_GCS_PATH_TEMPLATE
)