## Spark- Apache Airflow
Apache Spark is a solution that helps a lot with distributed data processing. To automate this task, a great solution is scheduling these tasks within Apache Airflow.Here shared are the steps i took to make this happen.

#### Content Table
* [Google Virtual machine instance set-up](https://github.com/Julius166083/Zcamp/blob/master/week_7_project/data_lake_ingestion/README.md#:~:text=seting%20up%20google%20VM%20instance)
* [General conceptual diagram](https://github.com/Julius166083/Zcamp/blob/master/week_7_project/data_lake_ingestion/README.md#:~:text=google%20VM%20instance-,General%20conceptual%20diagram,-(A)spark%20%26%20Jupyter)
* [Apache-airflow ](https://github.com/Julius166083/Zcamp/blob/master/week_7_project/data_lake_ingestion/README.md#:~:text=all%2Dspark%2Dnotebook-,(B)Apache%20Airflow,-Pre%2DReqs)
* [Spark & Jupyter notebook](https://github.com/Julius166083/Zcamp/blob/master/week_7_project/data_lake_ingestion/README.md#:~:text=(A)spark%20%26%20Jupyter%20notebook)
* [Executing Spark Jobs with Apache Airflow](https://github.com/Julius166083/Zcamp/blob/master/week_7_project/data_lake_ingestion/README.md#:~:text=webserver%20and%20Scheduler-,Executing%20Spark%20Jobs%20with%20Apache%20Airflow,-%C2%A9%202022%20GitHub%2C%20Inc)
--------------------------------------------------------------------------------------------------------------------------
### seting up google VM instance
Better google link for virtual machine set up been shared here
https://cloud.google.com/compute/docs/instances/create-start-instance
### General conceptual diagram
<img width="628" alt="airfloww" src="https://user-images.githubusercontent.com/87927403/161413601-73b5ab40-6114-4489-8c5f-71e234d1bf57.PNG">   

### (A)spark & Jupyter notebook
#### spark set-up
Set up spark and jupyter notebook image configurations as shown appended in my docker-compose.yaml file. In this file spark docker containers were set up
as follows

spark: Spark Master.
Image: bde2020/spark-master:3.1.1-hadoop3.2-java11
Port:  - "8081:8080"
       - "7077:7077"
References:
https://hub.docker.com/r/bde2020/spark-master

spark-worker-N: Spark workers. You can add workers copying the containers and changing the container name inside the docker-compose.yml file.In my case i created two spark-workers: "spark-worker-1" & "spark-worker-2"

Image: bde2020/spark-worker:3.1.1-hadoop3.2-java11
References:
https://hub.docker.com/r/bde2020/spark-worker

jupyter-spark: Jupyter notebook with pyspark for interactive development.
Image: jupyter/all-spark-notebook:java-11.0.14
Port:- "8888:8888"
     - "4040-4080:4040-4080"
References:
https://hub.docker.com/r/jupyter/all-spark-notebook

##### Requirements
Installing additional support packages, use one ot the following approaches
* create a requirement.txt file and list the packegaes you need to install such as pyarrow, spark-bigquery-with- dependencies_2.12-0.24.0.jar, gcs-connector-hadoop3-latest.jar and thereafter call the requirements.txt file in Dockerfile, as 
shown in my repo.
* You can directly put the additional packeages to install in the Dockerfile, like shown in my Dockerfile

           ADD https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar $SPARK_HOME/jars
           RUN chmod 774 $SPARK_HOME/jars/gcs-connector-hadoop3-latest.jar
    
* use the _PIP_ADDITIONAL_REQUIREMENTS: feature in docker-compose.yaml file to list support packages to be installed as shown 

      _PIP_ADDITIONAL_REQUIREMENTS: ${_PIP_ADDITIONAL_REQUIREMENTS:- apache-airflow-providers-apache-spark kaggle dbt-
    bigquery==1.0.0}
    
* usefull links for these packages include;
  gcs-connector-hadoop3-latest.jar:https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar 

  spark-bigquery-latest_2.12.jarhttps://storage.googleapis.com/spark-lib/bigquery/spark-bigquery-latest_2.12.jar 

  spark-bigquery-with-dependencies_2.12-0.24.0.jarhttps://storage.googleapis.com/spark-lib/bigquery/spark-bigquery-with-
  dependencies_2.12-0.24.0.jar

 
### (B)Apache Airflow
##### Airflow Setup
* Rename your gcp-service-accounts-credentials file to google_credentials.json & store it in your $HOME directory

    cd ~ && mkdir -p ~/.google/credentials/
    mv <path/to/your/service-account-authkeys>.json ~/.google/credentials/google_credentials.json

* Create a ".env" file and Set the Airflow user-id, To get rid of the warning ("AIRFLOW_UID is not set"), you can create .env 
  file with this content:
  
            AIRFLOW_UID=50000

* On Linux, the quick-start needs to know your host user-id and needs to have group id set to 0. Otherwise the files created in 
  dags, logs and plugins will be created with root user. You have to make sure to configure them for the docker-compose:
  
            mkdir -p ./dags ./logs ./plugins
            echo -e "AIRFLOW_UID=$(id -u)" > .env
            
* Create a script to handle GCP authentication, in my repo "entrypoint.sh" in the scripts folder was created as shown and 
  and thereafter called in the Dockerfile
* To install additiona requirement packages choose one of the folowing ways;
   * create a requirement.txt file and list the packegaes you need to install such as pyarrow, spark-bigquery-with- 
     dependencies_2.12-0.24.0.jar, gcs-connector-hadoop3-latest.jar and thereafter call the requirements.txt file in Dockerfile, 
     as shown in my repo.
   * You can directly put the additional packeages to install in the Dockerfile, like shown in my Dockerfile
   * use the _PIP_ADDITIONAL_REQUIREMENTS: feature in docker-compose.yaml file to list support packages to be installed as shown 
* Import the official docker-compose.yaml setup file from the latest Airflow version:

            curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'
            
* Create a Dockerfile pointing to Airflow version you've just downloaded, such as apache/airflow:2.2.3, as the base image,
  And customize this Dockerfile by:
  * Adding your custom packages to be installed. The one we'll need the most is gcloud to connect with the GCS 
    bucket/Data Lake.
  * Also, integrating requirements.txt to install libraries via pip install
* Docker Compose:
  Back in your docker-compose.yaml:
  In x-airflow-common:
  Remove the image tag, to replace it with your build from your Dockerfile, as shown
  Mount your google_credentials in volumes section as read-only
  Set environment variables: GCP_PROJECT_ID, GCP_GCS_BUCKET, GOOGLE_APPLICATION_CREDENTIALS & AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT,   as per your config.
  Change AIRFLOW__CORE__LOAD_EXAMPLES to false (optional)
  Kindly refer to my created samples of docker-compose.yaml and Dockerfile in my repo here
  
### (C) Docker builing
* Use the following docker compose commands to bring up all the docker containers of airflow and spark

              docker-compose build 
              docker-compose up
              
* when all is read and you want to stop the containers use
             docker-compose down --remove-orphans
             
* Accessing spark , airflow and jupyter, if your on a local machine access their GUIs at 
                Jupyter : https://localhost:8888
                Spark   :https://localhost:8081
                Airflow : https://localhost:8080
        
  If your using a google remote VM instance, do prot forwarding in visual studio, go to the terminal tab, click new terminal
  and when it opens up click, ports tab and then add ports to forward traffic to, thereafter access the services locally.
  
              Jupyter : https://localhost:8888
              Spark   :https://localhost:8081
             Airflow : https://localhost:8080, password=airflow, username=airflow

##### troubleshooting Problems
"File /.google/credentials/google_credentials.json was not found"
First, make sure you have your credentials in your $HOME/.google/credentials. Maybe you missed the step and didn't copy the your JSON with credentials there? Also, make sure the file-name is google_credentials.json.

Second, check that docker-compose can correctly map this directory to airflow worker.
Execute docker ps to see the list of docker containers running on your host machine and find the ID of the airflow worker.
Then execute bash on this container:
    
                 docker exec -it <container-ID> bash
    
Now check if the file with credentials is actually there:
    
                 ls -lh /.google/credentials/

If it's empty, docker-compose couldn't map the folder with credentials. In this case, try changing it to the absolute path to this folder:

  volumes:
    - ./dags:/opt/airflow/dags
    - ./logs:/opt/airflow/logs
    - ./plugins:/opt/airflow/plugins
    
    # here: ----------------------------
    - c:/home/user-name/.google/credentials/:/.google/credentials:ro
    # -----------------------------------
    
Airflow image to be used in the docker containers is got from the following link, [docker pull apache/airflow:2.2.4-python3.9](https://hub.docker.com/layers/airflow/apache/airflow/2.2.4-python3.9/images/sha256-66b6de33ec0d0147ff1802a5e1fd82eedbe950fa3293f3c2cd7d7e9c2079668b?context=explore)

It consists of the following docker containers that are built in one 'docker-compose.yaml" file download file from [link](https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml)

   $ curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'
   
* postgres: Postgres database for Airflow metadata and a Test database to test whatever you want
* airflow-webserver: Airflow webserver and Scheduler
       
### Executing Spark Jobs with Apache Airflow
Start by creating DAGs in Apache Airflow capable of running Apache Spark jobs
##### pre-Requisits summary
       - virtual machine host Apache airflow and spark used is linux ubuntu
       - configure JAVA_HOME environment, to enable you run spark on airfow using Pythonoperators and BashOperators,
         If you don???t have java installed, install it in your spark folder (ie ~/spark) with the following commands:
       
          sudo apt update
          wget https://download.java.net/java/GA/jdk11/9/GPL/openjdk-11.0.2_linux-x64_bin.tar.gz
          - Unpack it:

          tar xzfv openjdk-11.0.2_linux-x64_bin.tar.gz
          - define JAVA_HOME and add it to PATH:

           export JAVA_HOME="${HOME}/spark/jdk-11.0.2"
           export PATH="${JAVA_HOME}/bin:${PATH}"
          - check that it works:

            java --version
                       Output:

            openjdk 11.0.2 2019-01-15
            OpenJDK Runtime Environment 18.9 (build 11.0.2+9)
            OpenJDK 64-Bit Server VM 18.9 (build 11.0.2+9, mixed mode)
          - Remove the archive:

          - rm openjdk-11.0.2_linux-x64_bin.tar.gz
       
       - setup spark and airflow Dockerfile and Docker-compose.yaml file as shown and explained above.
       - In the DAGs folder under airflow folder create a dag script my case i named it "dag_spark_task.py" where you call a 
         SparkSubmitOperator ( kindly refer to READMR.md in DAGs folder for detailed explanation about each dag.
       

       
       
       
       
       
       
       
       
       
       
       
       
       
       
       
