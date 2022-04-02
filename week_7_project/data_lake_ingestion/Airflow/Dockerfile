# First-time build can take upto 10 mins.

##### installing GCS dependencies

#FROM image: apache/spark-py:v3.2.1
FROM gcr.io/spark-operator/spark@sha256:b1073fefa1442059544ce5ee657d174a9082c6e6d266347ce2f45c82b08a64b5
#FROM gcr.io/spark-operator/spark:v3.1.1
#FROM ${SPARK_IMAGE}

# Switch to user root so we can add additional jars and configuration files.
USER root

# Setup dependencies for Google Cloud Storage access.
RUN rm $SPARK_HOME/jars/guava-14.0.1.jar
ADD https://repo1.maven.org/maven2/com/google/guava/guava/23.0/guava-23.0.jar $SPARK_HOME/jars
RUN chmod 774 $SPARK_HOME/jars/guava-23.0.jar

# Add the connector jar needed to access Google Cloud Storage using the Hadoop FileSystem API.
ADD https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar $SPARK_HOME/jars
RUN chmod 774 $SPARK_HOME/jars/gcs-connector-hadoop3-latest.jar
ADD https://storage.googleapis.com/spark-lib/bigquery/spark-bigquery-latest_2.12.jar $SPARK_HOME/jars
RUN chmod 774 $SPARK_HOME/jars/spark-bigquery-latest_2.12.jar
ADD https://storage.googleapis.com/spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.24.0.jar $SPARK_HOME/jars
RUN chmod 774 $SPARK_HOME/jars/spark-bigquery-with-dependencies_2.12-0.24.0.jar



#=========================================================================
########################### Apache  airflow #############################
#=========================================================================

FROM apache/airflow:2.2.4

ENV AIRFLOW_HOME=/opt/airflow

USER root
RUN apt-get update -qq && apt-get install vim -qqq
RUN mkdir -p /.kaggle


COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


# Ref: https://airflow.apache.org/docs/docker-stack/recipes.html

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

ARG CLOUD_SDK_VERSION=322.0.0
ENV GCLOUD_HOME=/home/google-cloud-sdk

ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"

RUN DOWNLOAD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/google-cloud-sdk.tar.gz" \
    && mkdir -p "${GCLOUD_HOME}" \
    && tar xzf "${TMP_DIR}/google-cloud-sdk.tar.gz" -C "${GCLOUD_HOME}" --strip-components=1 \
    && "${GCLOUD_HOME}/install.sh" \
       --bash-completion=false \
       --path-update=false \
       --usage-reporting=false \
       --quiet \
    && rm -rf "${TMP_DIR}" \
    && gcloud --version

WORKDIR $AIRFLOW_HOME

COPY scripts scripts
RUN chmod +x scripts

USER airflow
RUN mkdir -p /home/airflow/.kaggle

USER root
RUN mkdir -p /home/airflow/.config
RUN mkdir -p /home/airflow/.config/gcloud

RUN mkdir -p /usr/local/spark_folder/resources
RUN mkdir -p /usr/local/spark_folder/resources/jars
RUN mkdir -p /usr/local/spark_folder/resources/energy_project
RUN chmod -R 777 /usr/local/spark_folder/resources
RUN chmod -R 777 /usr/local/spark_folder/resources/energy_project



USER $AIRFLOW_UID



