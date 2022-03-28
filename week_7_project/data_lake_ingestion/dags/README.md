## Data Ingestion plan
** used the Batch/workflow orchestration plan **
** end-to-end data pipeline
- create dockerfile and docker-compose.yaml files as shown in my git repo and specify details to install airflow
  image and other package modules as specified in a separate 'requirements.txt' file like; pyarrow, kaggle, apache-airflow-providers-google
- creat airflow directories of dags, plugins, logs and mount them on the airflow
  containers as specified in the docker-compose.yaml file
- created DAG scripts shown bellow
  ** 1st_dag_download_zipfile.py
  ** Dag_Ned_energy_consumption_ingestion.py: feeds the datalake with raw data
     files of 'Electricity' and 'Gas'
******** using the unzip command extract "dutch-energy.zip" file
          

- other important files created include
 - scripts.sh file:
 - .env  file:
 - requirements.txt: include; pyarrow, kaggle, apache-airflow
- Running spark jobs in airflow
 - edited the docker-compose.yaml file as shown in my git repo, special features to note

   volumes:
     postgres-db-volume:
     shared-workspace:
        name: "jordi_airflow"
        driver: local
        driver_opts:
          type: "none"
          o: "bind"
          device: "/Zcamp/week_7_Project/Datalake_ingestion/spark_folder"

   NB: the "/Zcamp/week_7_Project/Datalake_ingestion/spark_folder" is where you installed spark and java in your linux-ubuntu virtual machine
- Run docker compose commands on the terminal
  - docker-compose build
  - docker-compose up -d

## DWH plan
** Tables are partitioned and clustered done using bigquery in GCP, code for sql
   querries is in the "energy_consumption_sql.sql" file. 
## 

