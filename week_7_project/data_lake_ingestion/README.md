## Spark- Apache Airflow
#### Content Table
* Google Virtual machine instance set-up
* General conceptual diagram
* Apache-airflow 
* Spark & Jupyter notebook
* Executing Spark Jobs with Apache Airflow
### General conceptual diagram
<img width="628" alt="airfloww" src="https://user-images.githubusercontent.com/87927403/161413601-73b5ab40-6114-4489-8c5f-71e234d1bf57.PNG">   

### (A)spark & Jupyter notebook
#### Pre-Requisits
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


### (B)Apache Airflow
##### Pre-Reqs
To be consistent, rename your gcp-service-accounts-credentials file to google_credentials.json & store it in your $HOME directory

    cd ~ && mkdir -p ~/.google/credentials/
    mv <path/to/your/service-account-authkeys>.json ~/.google/credentials/google_credentials.json
    
##### Airflow Setup
* Create a new sub-directory called airflow in your project dir as shown mine was in 

* Set the Airflow user:  AIRFLOW_UID=50000

On Linux, the quick-start needs to know your host user-id and needs to have group id set to 0. Otherwise the files created in dags, logs and plugins will be created with root user. You have to make sure to configure them for the docker-compose:

            mkdir -p ./dags ./logs ./plugins
            echo -e "AIRFLOW_UID=$(id -u)" > .env

To get rid of the warning ("AIRFLOW_UID is not set"), you can create .env file with this content:

            AIRFLOW_UID=50000
* Import the official docker-compose.yaml setup file from the latest Airflow version:

            curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'
            
* Docker Build:
  Create a <Dockerfile> pointing to Airflow version you've just downloaded, such as apache/airflow:2.2.3, as the base image,
  And customize this Dockerfile by:
  * Adding your custom packages to be installed. The one we'll need the most is gcloud to connect with the GCS 
    bucket/Data Lake.
  * Also, integrating requirements.txt to install libraries via pip install
* Docker Compose:
Back in your docker-compose.yaml:

In x-airflow-common:
Remove the image tag, to replace it with your build from your Dockerfile, as shown
Mount your google_credentials in volumes section as read-only
Set environment variables: GCP_PROJECT_ID, GCP_GCS_BUCKET, GOOGLE_APPLICATION_CREDENTIALS & AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT, as per your config.
Change AIRFLOW__CORE__LOAD_EXAMPLES to false (optional)
Kindly refer to my created samples of docker-compose.yaml and Dockerfile in my repo here

Problems
File /.google/credentials/google_credentials.json was not found
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
    - c:/Users/alexe/.google/credentials/:/.google/credentials:ro
    # -----------------------------------
    
Airflow image to be used in the docker containers is got from the following link, [docker pull apache/airflow:2.2.4-python3.9](https://hub.docker.com/layers/airflow/apache/airflow/2.2.4-python3.9/images/sha256-66b6de33ec0d0147ff1802a5e1fd82eedbe950fa3293f3c2cd7d7e9c2079668b?context=explore)

It consists of the following docker containers that are built in one 'docker-compose.yaml" file download file from [link](https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml)

   $ curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'
   
* postgres: Postgres database for Airflow metadata and a Test database to test whatever you want
* airflow-webserver: Airflow webserver and Scheduler
* 