#!/usr/bin/env python
# coding: utf-8

# In[78]:


#importing useful libraries

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql.functions import col
from pyspark.sql.types import StringType,BooleanType,DateType,IntegerType,DoubleType
from pyspark.conf import SparkConf
from pyspark.context import SparkContext


import pandas as pd
import pyarrow.parquet as pq

# GCS Library
import gcsfs
from google.cloud import storage


# In[2]:


# spark,hadoop configuraturations to eneable access google cloud data before building a spark session
conf = SparkConf()     .setMaster('local[*]')     .setAppName('energy_consumption')     .set("spark.jars", "/usr/local/spark_folder/resources/jars/gcs-connector-hadoop3-latest.jar")     .set("spark.hadoop.google.cloud.auth.service.account.enable", "true")     .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/home/airflow/.config/gcloud/application_default_credentials.json")
sc = SparkContext(conf=conf)

sc._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
sc._jsc.hadoopConfiguration().set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
sc._jsc.hadoopConfiguration().set("fs.gs.auth.service.account.json.keyfile", "/home/airflow/.config/gcloud/application_default_credentials.json")
sc._jsc.hadoopConfiguration().set("fs.gs.auth.service.account.enable", "true")


# In[3]:


#building spark session to call configurations set into action
spark = SparkSession.builder     .config(conf=sc.getConf())     .getOrCreate()


# In[75]:


# pulling parquet data from google cloud storage, with correct positioning of column names
# Scope will cover 4 network managers of (Coteq,Enexis, Liander & Stedin for a period of 5 years from 2013 -2017)

# Network mananager:COTEQ: 
############################# Electricity: 2013-2017########
df1_coteq_electrical_2013 = spark.read     .option("header", "true")     .parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/ELECTRICITY_COTEQ/coteq_electricity_2013.parquet')

df1_coteq_electrical_2014 = spark.read     .option("header", "true")     .parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/ELECTRICITY_COTEQ/coteq_electricity_2014.parquet')

df1_coteq_electrical_2015 = spark.read     .option("header", "true")     .parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/ELECTRICITY_COTEQ/coteq_electricity_2015.parquet')

df1_coteq_electrical_2016 = spark.read     .option("header", "true")     .parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/ELECTRICITY_COTEQ/coteq_electricity_2016.parquet')

df1_coteq_electrical_2017 = spark.read     .option("header", "true")     .parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/ELECTRICITY_COTEQ/coteq_electricity_2017.parquet')

########################## Gas:2013-2017 #####################
df1_coteq_gas_2013 = spark.read     .option("header", "true")     .parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/GAS_COTEQ/coteq_gas_2013.parquet')

df1_coteq_gas_2014 = spark.read     .option("header", "true")     .parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/GAS_COTEQ/coteq_gas_2014.parquet')

df1_coteq_gas_2015 = spark.read     .option("header", "true")     .parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/GAS_COTEQ/coteq_gas_2015.parquet')

df1_coteq_gas_2016 = spark.read     .option("header", "true")     .parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/GAS_COTEQ/coteq_gas_2016.parquet')

df1_coteq_gas_2017 = spark.read     .option("header", "true")     .parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/GAS_COTEQ/coteq_gas_2017.parquet')


# Network mananager:STEDIN: 
############################# Electricity: 2013-2017########
df1_stedin_electrical_2013 = spark.read     .option("header", "true")     .parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/ELECTRICITY_STEDIN/stedin_electricity_2013.parquet')

df1_stedin_electrical_2014 = spark.read     .option("header", "true")     .parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/ELECTRICITY_STEDIN/stedin_electricity_2014.parquet')

df1_stedin_electrical_2015 = spark.read     .option("header", "true")     .parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/ELECTRICITY_STEDIN/stedin_electricity_2015.parquet')

df1_stedin_electrical_2016 = spark.read     .option("header", "true")     .parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/ELECTRICITY_STEDIN/stedin_electricity_2016.parquet')

df1_stedin_electrical_2017 = spark.read     .option("header", "true")     .parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/ELECTRICITY_STEDIN/stedin_electricity_2017.parquet')

            ########################## Gas ##############################
df1_stedin_gas_2013 = spark.read     .option("header", "true")     .parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/GAS_STEDIN/stedin_gas_2013.parquet')

df1_stedin_gas_2014 = spark.read     .option("header", "true")     .parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/GAS_STEDIN/stedin_gas_2014.parquet')

df1_stedin_gas_2015 = spark.read     .option("header", "true")     .parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/GAS_STEDIN/stedin_gas_2015.parquet')

df1_stedin_gas_2016 = spark.read     .option("header", "true")     .parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/GAS_STEDIN/stedin_gas_2016.parquet')

df1_stedin_gas_2017 = spark.read     .option("header", "true")     .parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/GAS_STEDIN/stedin_gas_2017.parquet')

# Network mananager:ENEXIS: 
############################# Electricity: 2013-2017########
df1_enexis_electrical_2013 = spark.read     .option("header", "true")     .parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/ELECTRICITY_ENEXIS/enexis_electricity_01012013.parquet')

df1_enexis_electrical_2014 = spark.read     .option("header", "true")     .parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/ELECTRICITY_ENEXIS/enexis_electricity_01012014.parquet')

df1_enexis_electrical_2015 = spark.read     .option("header", "true")     .parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/ELECTRICITY_ENEXIS/enexis_electricity_01012015.parquet')

df1_enexis_electrical_2016 = spark.read     .option("header", "true")     .parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/ELECTRICITY_ENEXIS/enexis_electricity_01012016.parquet')

df1_enexis_electrical_2017 = spark.read     .option("header", "true")     .parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/ELECTRICITY_ENEXIS/enexis_electricity_01012016.parquet')

            ########################## Gas ##############################
df1_enexis_gas_2013 = spark.read     .option("header", "true")     .parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/GAS_ENEXIS/enexis_gas_01012013.parquet')

df1_enexis_gas_2014 = spark.read     .option("header", "true")     .parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/GAS_ENEXIS/enexis_gas_01012014.parquet')

df1_enexis_gas_2015 = spark.read     .option("header", "true")     .parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/GAS_ENEXIS/enexis_gas_01012015.parquet')

df1_enexis_gas_2016 = spark.read     .option("header", "true")     .parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/GAS_ENEXIS/enexis_gas_01012016.parquet')

df1_enexis_gas_2017 = spark.read     .option("header", "true")     .parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/GAS_ENEXIS/enexis_gas_01012017.parquet')

# Network mananager:LIANDER: 
############################# Electricity: 2013-2017########
df1_liander_electrical_2013 = spark.read     .option("header", "true")     .parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/ELECTRICITY_LIANDER/liander_electricity_01012013.parquet')

df1_liander_electrical_2014 = spark.read     .option("header", "true")     .parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/ELECTRICITY_LIANDER/liander_electricity_01012014.parquet')

df1_liander_electrical_2015 = spark.read     .option("header", "true")     .parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/ELECTRICITY_LIANDER/liander_electricity_01012015.parquet')

df1_liander_electrical_2016 = spark.read     .option("header", "true")     .parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/ELECTRICITY_LIANDER/liander_electricity_01012016.parquet')

df1_liander_electrical_2017 = spark.read     .option("header", "true")     .parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/ELECTRICITY_LIANDER/liander_electricity_01012017.parquet')

            ########################## Gas ##############################
df1_liander_gas_2013 = spark.read     .option("header", "true")     .parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/GAS_LIANDER/liander_gas_01012013.parquet')

df1_liander_gas_2014 = spark.read     .option("header", "true")     .parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/GAS_LIANDER/liander_gas_01012014.parquet')

df1_liander_gas_2015 = spark.read     .option("header", "true")     .parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/GAS_LIANDER/liander_gas_01012015.parquet')

df1_liander_gas_2016 = spark.read     .option("header", "true")     .parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/GAS_LIANDER/liander_gas_01012016.parquet')

df1_liander_gas_2017 = spark.read     .option("header", "true")     .parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/GAS_LIANDER/liander_gas_01012017.parquet')


# In[76]:


# generatting spark dataframes
df1_coteq_electrical_2013.printSchema()
df1_coteq_electrical_2014.printSchema()
df1_coteq_electrical_2015.printSchema()
df1_coteq_electrical_2016.printSchema()
df1_coteq_electrical_2017.printSchema()
df1_coteq_gas_2013.printSchema()
df1_coteq_gas_2014.printSchema()
df1_coteq_gas_2015.printSchema()
df1_coteq_gas_2016.printSchema()
df1_coteq_gas_2017.printSchema()
df1_stedin_electrical_2013.printSchema() 
df1_stedin_electrical_2014.printSchema()
df1_stedin_electrical_2015.printSchema()
df1_stedin_electrical_2016.printSchema()
df1_stedin_electrical_2017.printSchema()
df1_stedin_gas_2013.printSchema()
df1_stedin_gas_2014.printSchema()
df1_stedin_gas_2015.printSchema()
df1_stedin_gas_2016.printSchema()
df1_stedin_gas_2017.printSchema()
df1_enexis_electrical_2013.printSchema()
df1_enexis_electrical_2014.printSchema() 
df1_enexis_electrical_2015.printSchema()
df1_enexis_electrical_2016.printSchema()
df1_enexis_electrical_2017.printSchema()
df1_enexis_gas_2013.printSchema() 
df1_enexis_gas_2014.printSchema()
df1_enexis_gas_2015.printSchema() 
df1_enexis_gas_2016.printSchema()
df1_enexis_gas_2017.printSchema()
df1_liander_electrical_2013.printSchema()
df1_liander_electrical_2014.printSchema()
df1_liander_electrical_2015.printSchema()
df1_liander_electrical_2016.printSchema()
df1_liander_electrical_2017.printSchema()
df1_liander_gas_2013.printSchema()
df1_liander_gas_2014.printSchema()
df1_liander_gas_2015.printSchema() 
df1_liander_gas_2016.printSchema()
df1_liander_gas_2017.printSchema()


# In[77]:


######COTEQ#####
df1_cast_coteq_electricity2013 = df1_coteq_electrical_2013.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df1_cast_coteq_electricity2013.printSchema()

df1_cast_coteq_electricity2014 = df1_coteq_electrical_2014.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df1_cast_coteq_electricity2014.printSchema()

df1_cast_coteq_electricity2015 = df1_coteq_electrical_2015.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df1_cast_coteq_electricity2015.printSchema()

df1_cast_coteq_electricity2016 = df1_coteq_electrical_2016.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df1_cast_coteq_electricity2016.printSchema()

df1_cast_coteq_electricity2017 = df1_coteq_electrical_2017.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df1_cast_coteq_electricity2017.printSchema()

df1_cast_coteq_gas2013 = df1_coteq_gas_2013.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df1_cast_coteq_gas2013.printSchema()
df1_cast_coteq_gas2014 = df1_coteq_gas_2014.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df1_cast_coteq_gas2014.printSchema()
df1_cast_coteq_gas2015 = df1_coteq_gas_2015.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df1_cast_coteq_gas2015.printSchema()
df1_cast_coteq_gas2016 = df1_coteq_gas_2016.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df1_cast_coteq_gas2016.printSchema()
df1_cast_coteq_gas2017 = df1_coteq_gas_2017.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df1_cast_coteq_gas2017.printSchema()

#######STEDIN
df1_cast_stedin_electricity2013 = df1_stedin_electrical_2013.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df1_cast_stedin_electricity2013.printSchema()
df1_cast_stedin_electricity2014 = df1_stedin_electrical_2014.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df1_cast_stedin_electricity2014.printSchema()
df1_cast_stedin_electricity2015 = df1_stedin_electrical_2015.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df1_cast_stedin_electricity2015.printSchema()
df1_cast_stedin_electricity2016 = df1_stedin_electrical_2016.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df1_cast_stedin_electricity2016.printSchema()
df1_cast_stedin_electricity2017 = df1_stedin_electrical_2017.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df1_cast_stedin_electricity2017.printSchema()
###
df1_cast_stedin_gas2013 = df1_stedin_gas_2013.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df1_cast_stedin_gas2013.printSchema()
df1_cast_stedin_gas2014 = df1_stedin_gas_2014.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df1_cast_stedin_gas2014.printSchema()
df1_cast_stedin_gas2015 = df1_stedin_gas_2015.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df1_cast_stedin_gas2015.printSchema()
df1_cast_stedin_gas2016 = df1_stedin_gas_2016.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df1_cast_stedin_gas2016.printSchema()
df1_cast_stedin_gas2017 = df1_stedin_gas_2017.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df1_cast_stedin_gas2017.printSchema()

##ENEXIS
df1_cast_enexis_electricity2013 = df1_enexis_electrical_2013.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df1_cast_enexis_electricity2013.printSchema()
df1_cast_enexis_electricity2014 = df1_enexis_electrical_2014.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df1_cast_enexis_electricity2014.printSchema()
df1_cast_enexis_electricity2015 = df1_enexis_electrical_2015.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df1_cast_enexis_electricity2015.printSchema()
df1_cast_enexis_electricity2016 = df1_enexis_electrical_2016.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df1_cast_enexis_electricity2016.printSchema()
df1_cast_enexis_electricity2017 = df1_enexis_electrical_2017.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df1_cast_enexis_electricity2017.printSchema()
###
df1_cast_enexis_gas2013 = df1_enexis_gas_2013.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df1_cast_enexis_gas2013.printSchema()
df1_cast_enexis_gas2014 = df1_enexis_gas_2014.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df1_cast_enexis_gas2014.printSchema()
df1_cast_enexis_gas2015 = df1_enexis_gas_2015.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df1_cast_enexis_gas2015.printSchema()
df1_cast_enexis_gas2016 = df1_enexis_gas_2016.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df1_cast_enexis_gas2016.printSchema()
df1_cast_enexis_gas2017 = df1_enexis_gas_2017.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df1_cast_enexis_gas2017.printSchema()

### LIANDER
df1_cast_liander_electricity2013 = df1_liander_electrical_2013.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df1_cast_liander_electricity2013.printSchema()
df1_cast_liander_electricity2014 = df1_liander_electrical_2014.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df1_cast_liander_electricity2014.printSchema()
df1_cast_liander_electricity2015 = df1_liander_electrical_2015.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df1_cast_liander_electricity2015.printSchema()
df1_cast_liander_electricity2016 = df1_liander_electrical_2016.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df1_cast_liander_electricity2016.printSchema()
df1_cast_liander_electricity2017 = df1_liander_electrical_2017.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df1_cast_liander_electricity2017.printSchema()
###
df1_cast_liander_gas2013 = df1_liander_gas_2013.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df1_cast_liander_gas2013.printSchema()
df1_cast_liander_gas2014 = df1_liander_gas_2014.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df1_cast_liander_gas2014.printSchema()
df1_cast_liander_gas2015 = df1_liander_gas_2015.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df1_cast_liander_gas2015.printSchema()
df1_cast_liander_gas2016 = df1_liander_gas_2016.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df1_cast_liander_gas2016.printSchema()
df1_cast_liander_gas2017 = df1_liander_gas_2017.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df1_cast_liander_gas2017.printSchema()


# #### Converting into pandas tables for easy: data exploration of missing values, merging & creation of columns

# In[129]:


df2_coteq_electrical_2013 =pd.read_parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/ELECTRICITY_COTEQ/coteq_electricity_2013.parquet', engine='pyarrow')
df2_coteq_electrical_2014 =pd.read_parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/ELECTRICITY_COTEQ/coteq_electricity_2014.parquet', engine='pyarrow')
df2_coteq_electrical_2015 =pd.read_parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/ELECTRICITY_COTEQ/coteq_electricity_2015.parquet', engine='pyarrow')
df2_coteq_electrical_2016 =pd.read_parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/ELECTRICITY_COTEQ/coteq_electricity_2016.parquet', engine='pyarrow')
df2_coteq_electrical_2017 =pd.read_parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/ELECTRICITY_COTEQ/coteq_electricity_2017.parquet', engine='pyarrow')
df2_coteq_gas_2013 =pd.read_parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/GAS_COTEQ/coteq_gas_2013.parquet', engine='pyarrow')
df2_coteq_gas_2014 =pd.read_parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/GAS_COTEQ/coteq_gas_2014.parquet', engine='pyarrow')
df2_coteq_gas_2015 =pd.read_parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/GAS_COTEQ/coteq_gas_2015.parquet', engine='pyarrow')
df2_coteq_gas_2016 =pd.read_parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/GAS_COTEQ/coteq_gas_2016.parquet', engine='pyarrow')
df2_coteq_gas_2017 =pd.read_parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/GAS_COTEQ/coteq_gas_2017.parquet', engine='pyarrow')
df2_stedin_electrical_2013 =pd.read_parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/ELECTRICITY_STEDIN/stedin_electricity_2013.parquet', engine='pyarrow')
df2_stedin_electrical_2014 =pd.read_parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/ELECTRICITY_STEDIN/stedin_electricity_2014.parquet', engine='pyarrow')
df2_stedin_electrical_2015 =pd.read_parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/ELECTRICITY_STEDIN/stedin_electricity_2015.parquet', engine='pyarrow')
df2_stedin_electrical_2016 =pd.read_parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/ELECTRICITY_STEDIN/stedin_electricity_2016.parquet', engine='pyarrow')
df2_stedin_electrical_2017 =pd.read_parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/ELECTRICITY_STEDIN/stedin_electricity_2017.parquet', engine='pyarrow')
df2_stedin_gas_2013 =pd.read_parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/GAS_STEDIN/stedin_gas_2013.parquet', engine='pyarrow')
df2_stedin_gas_2014 =pd.read_parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/GAS_STEDIN/stedin_gas_2014.parquet', engine='pyarrow')
df2_stedin_gas_2015 =pd.read_parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/GAS_STEDIN/stedin_gas_2015.parquet', engine='pyarrow')
df2_stedin_gas_2016 =pd.read_parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/GAS_STEDIN/stedin_gas_2016.parquet', engine='pyarrow')
df2_stedin_gas_2017 =pd.read_parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/GAS_STEDIN/stedin_gas_2017.parquet', engine='pyarrow')
df2_enexis_electrical_2013 =pd.read_parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/ELECTRICITY_ENEXIS/enexis_electricity_01012013.parquet', engine='pyarrow')
df2_enexis_electrical_2014 =pd.read_parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/ELECTRICITY_ENEXIS/enexis_electricity_01012014.parquet', engine='pyarrow')
df2_enexis_electrical_2015 =pd.read_parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/ELECTRICITY_ENEXIS/enexis_electricity_01012015.parquet', engine='pyarrow')
df2_enexis_electrical_2016 =pd.read_parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/ELECTRICITY_ENEXIS/enexis_electricity_01012016.parquet', engine='pyarrow')
df2_enexis_electrical_2017 =pd.read_parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/ELECTRICITY_ENEXIS/enexis_electricity_01012016.parquet', engine='pyarrow')
df2_enexis_gas_2013 =pd.read_parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/GAS_ENEXIS/enexis_gas_01012013.parquet', engine='pyarrow')
df2_enexis_gas_2014 =pd.read_parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/GAS_ENEXIS/enexis_gas_01012014.parquet', engine='pyarrow')
df2_enexis_gas_2015 =pd.read_parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/GAS_ENEXIS/enexis_gas_01012015.parquet', engine='pyarrow')
df2_enexis_gas_2016 =pd.read_parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/GAS_ENEXIS/enexis_gas_01012016.parquet', engine='pyarrow')
df2_enexis_gas_2017 =pd.read_parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/GAS_ENEXIS/enexis_gas_01012017.parquet', engine='pyarrow')
df2_liander_electrical_2013 =pd.read_parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/ELECTRICITY_LIANDER/liander_electricity_01012013.parquet', engine='pyarrow')
df2_liander_electrical_2014 =pd.read_parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/ELECTRICITY_LIANDER/liander_electricity_01012014.parquet', engine='pyarrow')
df2_liander_electrical_2015 =pd.read_parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/ELECTRICITY_LIANDER/liander_electricity_01012015.parquet', engine='pyarrow')
df2_liander_electrical_2016 =pd.read_parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/ELECTRICITY_LIANDER/liander_electricity_01012016.parquet', engine='pyarrow')
df2_liander_electrical_2017 =pd.read_parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/ELECTRICITY_LIANDER/liander_electricity_01012017.parquet', engine='pyarrow')
df2_liander_gas_2013 =pd.read_parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/GAS_LIANDER/liander_gas_01012013.parquet', engine='pyarrow')
df2_liander_gas_2014 =pd.read_parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/GAS_LIANDER/liander_gas_01012014.parquet', engine='pyarrow')
df2_liander_gas_2015 =pd.read_parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/GAS_LIANDER/liander_gas_01012015.parquet', engine='pyarrow')
df2_liander_gas_2016 =pd.read_parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/GAS_LIANDER/liander_gas_01012016.parquet', engine='pyarrow')
df2_liander_gas_2017 =pd.read_parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/GAS_LIANDER/liander_gas_01012017.parquet', engine='pyarrow')


# # Explore & fill missing values in data sets

# In[130]:


print("df2_coteq_electrical_2013")
df2_coteq_electrical_2013.info()
print("df2_coteq_electrical_2014")
df2_coteq_electrical_2014.info()
print("df2_coteq_electrical_2015")
df2_coteq_electrical_2015.info()
print("df2_coteq_electrical_2016")
df2_coteq_electrical_2016.info()
print("df2_coteq_electrical_2017")
df2_coteq_electrical_2017.info()
print("df2_coteq_gas_2013")
df2_coteq_gas_2013.info()
print("df2_coteq_gas_2014")
df2_coteq_gas_2014.info()
print("df2_coteq_gas_2015")
df2_coteq_gas_2015.info()
print("df2_coteq_gas_2016")
df2_coteq_gas_2016.info()
print("df2_coteq_gas_2017")
df2_coteq_gas_2017.info()
print("df2_stedin_electrical_2013")
df2_stedin_electrical_2013.info()
print("df2_stedin_electrical_2014")
df2_stedin_electrical_2014.info()
print("df2_stedin_electrical_2015")
df2_stedin_electrical_2015.info()
print("df2_stedin_electrical_2016")
df2_stedin_electrical_2016.info()
print("df2_stedin_electrical_2017")
df2_stedin_electrical_2017.info()
print("df2_stedin_gas_2013")
df2_stedin_gas_2013.info()
print("df2_stedin_gas_2014")
df2_stedin_gas_2014.info()
print("df2_stedin_gas_2015")
df2_stedin_gas_2015.info()
print("df2_stedin_gas_2016")
df2_stedin_gas_2016.info()
print("df2_stedin_gas_2017")
df2_stedin_gas_2017.info()
print("df2_enexis_electrical_2013")
df2_enexis_electrical_2013.info()
print("df2_enexis_electrical_2014")
df2_enexis_electrical_2014.info()
print("df2_enexis_electrical_2015")
df2_enexis_electrical_2015.info()
print("df2_enexis_electrical_2016")
df2_enexis_electrical_2016.info()
print("df2_enexis_electrical_2017")
df2_enexis_electrical_2017.info()
print("df2_enexis_gas_2013")
df2_enexis_gas_2013.info()
print("df2_enexis_gas_2014")
df2_enexis_gas_2014.info()
print("df2_enexis_gas_2015")
df2_enexis_gas_2015.info()
print("df2_enexis_gas_2016")
df2_enexis_gas_2016.info()
print("df2_enexis_gas_2017")
df2_enexis_gas_2017.info()
print("df2_liander_electrical_2013")
df2_liander_electrical_2013.info()
print("df2_liander_electrical_2014")
df2_liander_electrical_2014.info()
print("df2_liander_electrical_2015")
df2_liander_electrical_2015.info()
print("df2_liander_electrical_2016")
df2_liander_electrical_2016.info()
print("df2_liander_electrical_2017")
df2_liander_electrical_2017.info()
print("df2_liander_gas_2013")
df2_liander_gas_2013.info()
print("df2_liander_gas_2014")
df2_liander_gas_2014.info()
print("df2_liander_gas_2015")
df2_liander_gas_2015.info()
print("df2_liander_gas_2016")
df2_liander_gas_2016.info()
print("df2_liander_gas_2017")
df2_liander_gas_2017.info()


# In[131]:


## filling in missing values in "liander_gas_2017"

df2_liander_gas_2017["purchase_area"].fillna("GAS Gastransport Services (GASUNIE)", inplace=True)


# In[134]:


## filling in missing values in "df2_enexis_gas_2013-1027"
df2_enexis_gas_2013["smartmeter_perc"].fillna("0", inplace=True)
df2_enexis_gas_2014["smartmeter_perc"].fillna("0", inplace=True)
df2_enexis_gas_2015["smartmeter_perc"].fillna("0", inplace=True)
df2_enexis_gas_2016["smartmeter_perc"].fillna("0", inplace=True)
df2_enexis_gas_2017["smartmeter_perc"].fillna("0", inplace=True)


# In[135]:


## verifying to see if all gaps are filled
print("df2_coteq_electrical_2013")
df2_coteq_electrical_2013.info()
print("df2_coteq_electrical_2014")
df2_coteq_electrical_2014.info()
print("df2_coteq_electrical_2015")
df2_coteq_electrical_2015.info()
print("df2_coteq_electrical_2016")
df2_coteq_electrical_2016.info()
print("df2_coteq_electrical_2017")
df2_coteq_electrical_2017.info()
print("df2_coteq_gas_2013")
df2_coteq_gas_2013.info()
print("df2_coteq_gas_2014")
df2_coteq_gas_2014.info()
print("df2_coteq_gas_2015")
df2_coteq_gas_2015.info()
print("df2_coteq_gas_2016")
df2_coteq_gas_2016.info()
print("df2_coteq_gas_2017")
df2_coteq_gas_2017.info()
print("df2_stedin_electrical_2013")
df2_stedin_electrical_2013.info()
print("df2_stedin_electrical_2014")
df2_stedin_electrical_2014.info()
print("df2_stedin_electrical_2015")
df2_stedin_electrical_2015.info()
print("df2_stedin_electrical_2016")
df2_stedin_electrical_2016.info()
print("df2_stedin_electrical_2017")
df2_stedin_electrical_2017.info()
print("df2_stedin_gas_2013")
df2_stedin_gas_2013.info()
print("df2_stedin_gas_2014")
df2_stedin_gas_2014.info()
print("df2_stedin_gas_2015")
df2_stedin_gas_2015.info()
print("df2_stedin_gas_2016")
df2_stedin_gas_2016.info()
print("df2_stedin_gas_2017")
df2_stedin_gas_2017.info()
print("df2_enexis_electrical_2013")
df2_enexis_electrical_2013.info()
print("df2_enexis_electrical_2014")
df2_enexis_electrical_2014.info()
print("df2_enexis_electrical_2015")
df2_enexis_electrical_2015.info()
print("df2_enexis_electrical_2016")
df2_enexis_electrical_2016.info()
print("df2_enexis_electrical_2017")
df2_enexis_electrical_2017.info()
print("df2_enexis_gas_2013")
df2_enexis_gas_2013.info()
print("df2_enexis_gas_2014")
df2_enexis_gas_2014.info()
print("df2_enexis_gas_2015")
df2_enexis_gas_2015.info()
print("df2_enexis_gas_2016")
df2_enexis_gas_2016.info()
print("df2_enexis_gas_2017")
df2_enexis_gas_2017.info()
print("df2_liander_electrical_2013")
df2_liander_electrical_2013.info()
print("df2_liander_electrical_2014")
df2_liander_electrical_2014.info()
print("df2_liander_electrical_2015")
df2_liander_electrical_2015.info()
print("df2_liander_electrical_2016")
df2_liander_electrical_2016.info()
print("df2_liander_electrical_2017")
df2_liander_electrical_2017.info()
print("df2_liander_gas_2013")
df2_liander_gas_2013.info()
print("df2_liander_gas_2014")
df2_liander_gas_2014.info()
print("df2_liander_gas_2015")
df2_liander_gas_2015.info()
print("df2_liander_gas_2016")
df2_liander_gas_2016.info()
print("df2_liander_gas_2017")
df2_liander_gas_2017.info()


# # Converting back pandas DataFrame to Spark DataFrame
# 
# 

# Due to parallel execution on all cores on multiple machines, PySpark runs operations faster than Pandas, 
# hence we often required to covert Pandas DataFrame to PySpark (Spark with Python) for better performance.

# In[136]:


df3_coteq_electrical_2013=spark.createDataFrame(df2_coteq_electrical_2013)
df3_coteq_electrical_2014=spark.createDataFrame(df2_coteq_electrical_2014)
df3_coteq_electrical_2015=spark.createDataFrame(df2_coteq_electrical_2015)
df3_coteq_electrical_2016=spark.createDataFrame(df2_coteq_electrical_2016)
df3_coteq_electrical_2017=spark.createDataFrame(df2_coteq_electrical_2017)
df3_coteq_gas_2013=spark.createDataFrame(df2_coteq_gas_2013)
df3_coteq_gas_2014=spark.createDataFrame(df2_coteq_gas_2014)
df3_coteq_gas_2015=spark.createDataFrame(df2_coteq_gas_2015)
df3_coteq_gas_2016=spark.createDataFrame(df2_coteq_gas_2016)
df3_coteq_gas_2017=spark.createDataFrame(df2_coteq_gas_2017)
df3_stedin_electrical_2013=spark.createDataFrame(df2_stedin_electrical_2013)
df3_stedin_electrical_2014=spark.createDataFrame(df2_stedin_electrical_2014)
df3_stedin_electrical_2015=spark.createDataFrame(df2_stedin_electrical_2015)
df3_stedin_electrical_2016=spark.createDataFrame(df2_stedin_electrical_2016)
df3_stedin_electrical_2017=spark.createDataFrame(df2_stedin_electrical_2017)
df3_stedin_gas_2013=spark.createDataFrame(df2_stedin_gas_2013)
df3_stedin_gas_2014=spark.createDataFrame(df2_stedin_gas_2014)
df3_stedin_gas_2015=spark.createDataFrame(df2_stedin_gas_2015)
df3_stedin_gas_2016=spark.createDataFrame(df2_stedin_gas_2016)
df3_stedin_gas_2017=spark.createDataFrame(df2_stedin_gas_2017)
df3_enexis_electrical_2013=spark.createDataFrame(df2_enexis_electrical_2013)
df3_enexis_electrical_2014=spark.createDataFrame(df2_enexis_electrical_2014)
df3_enexis_electrical_2015=spark.createDataFrame(df2_enexis_electrical_2015)
df3_enexis_electrical_2016=spark.createDataFrame(df2_enexis_electrical_2016)
df3_enexis_electrical_2017=spark.createDataFrame(df2_enexis_electrical_2017)
df3_enexis_gas_2013=spark.createDataFrame(df2_enexis_gas_2013)
df3_enexis_gas_2014=spark.createDataFrame(df2_enexis_gas_2014)
df3_enexis_gas_2015=spark.createDataFrame(df2_enexis_gas_2015)
df3_enexis_gas_2016=spark.createDataFrame(df2_enexis_gas_2016)
df3_enexis_gas_2017=spark.createDataFrame(df2_enexis_gas_2017)
df3_liander_electrical_2013=spark.createDataFrame(df2_liander_electrical_2013)
df3_liander_electrical_2014=spark.createDataFrame(df2_liander_electrical_2014)
df3_liander_electrical_2015=spark.createDataFrame(df2_liander_electrical_2015)
df3_liander_electrical_2016=spark.createDataFrame(df2_liander_electrical_2016)
df3_liander_electrical_2017=spark.createDataFrame(df2_liander_electrical_2017)
df3_liander_gas_2013=spark.createDataFrame(df2_liander_gas_2013)
df3_liander_gas_2014=spark.createDataFrame(df2_liander_gas_2014)
df3_liander_gas_2015=spark.createDataFrame(df2_liander_gas_2015)
df3_liander_gas_2016=spark.createDataFrame(df2_liander_gas_2016)
df3_liander_gas_2017=spark.createDataFrame(df2_liander_gas_2017)


# In[147]:


#editing spark datafame
######COTEQ#####
df3_cast_coteq_electricity2013 = df3_coteq_electrical_2013.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df3_cast_coteq_electricity2013.printSchema()

df3_cast_coteq_electricity2014 = df3_coteq_electrical_2014.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df3_cast_coteq_electricity2014.printSchema()

df3_cast_coteq_electricity2015 = df3_coteq_electrical_2015.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df3_cast_coteq_electricity2015.printSchema()

df3_cast_coteq_electricity2016 = df3_coteq_electrical_2016.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df3_cast_coteq_electricity2016.printSchema()

df3_cast_coteq_electricity2017 = df3_coteq_electrical_2017.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df3_cast_coteq_electricity2017.printSchema()

df3_cast_coteq_gas2013 = df3_coteq_gas_2013.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df3_cast_coteq_gas2013.printSchema()
df3_cast_coteq_gas2014 = df3_coteq_gas_2014.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df3_cast_coteq_gas2014.printSchema()
df3_cast_coteq_gas2015 = df3_coteq_gas_2015.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df3_cast_coteq_gas2015.printSchema()
df3_cast_coteq_gas2016 = df3_coteq_gas_2016.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df3_cast_coteq_gas2016.printSchema()
df3_cast_coteq_gas2017 = df3_coteq_gas_2017.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df3_cast_coteq_gas2017.printSchema()

#######STEDIN
df3_cast_stedin_electricity2013 = df3_stedin_electrical_2013.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df3_cast_stedin_electricity2013.printSchema()
df3_cast_stedin_electricity2014 = df3_stedin_electrical_2014.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df3_cast_stedin_electricity2014.printSchema()
df3_cast_stedin_electricity2015 = df3_stedin_electrical_2015.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df3_cast_stedin_electricity2015.printSchema()
df3_cast_stedin_electricity2016 = df3_stedin_electrical_2016.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df3_cast_stedin_electricity2016.printSchema()
df3_cast_stedin_electricity2017 = df3_stedin_electrical_2017.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df3_cast_stedin_electricity2017.printSchema()
###
df3_cast_stedin_gas2013 = df3_stedin_gas_2013.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df3_cast_stedin_gas2013.printSchema()
df3_cast_stedin_gas2014 = df3_stedin_gas_2014.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df3_cast_stedin_gas2014.printSchema()
df3_cast_stedin_gas2015 = df3_stedin_gas_2015.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df3_cast_stedin_gas2015.printSchema()
df3_cast_stedin_gas2016 = df3_stedin_gas_2016.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df3_cast_stedin_gas2016.printSchema()
df3_cast_stedin_gas2017 = df3_stedin_gas_2017.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df3_cast_stedin_gas2017.printSchema()

##ENEXIS
df3_cast_enexis_electricity2013 = df3_enexis_electrical_2013.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df3_cast_enexis_electricity2013.printSchema()
df3_cast_enexis_electricity2014 = df3_enexis_electrical_2014.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df3_cast_enexis_electricity2014.printSchema()
df3_cast_enexis_electricity2015 = df3_enexis_electrical_2015.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df3_cast_enexis_electricity2015.printSchema()
df3_cast_enexis_electricity2016 = df3_enexis_electrical_2016.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df3_cast_enexis_electricity2016.printSchema()
df3_cast_enexis_electricity2017 = df3_enexis_electrical_2017.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df3_cast_enexis_electricity2017.printSchema()
###
df3_cast_enexis_gas2013 = df3_enexis_gas_2013.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df3_cast_enexis_gas2013.printSchema()
df3_cast_enexis_gas2014 = df3_enexis_gas_2014.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df3_cast_enexis_gas2014.printSchema()
df3_cast_enexis_gas2015 = df3_enexis_gas_2015.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df3_cast_enexis_gas2015.printSchema()
df3_cast_enexis_gas2016 = df3_enexis_gas_2016.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df3_cast_enexis_gas2016.printSchema()
df3_cast_enexis_gas2017 = df3_enexis_gas_2017.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df3_cast_enexis_gas2017.printSchema()

### LIANDER
df3_cast_liander_electricity2013 = df3_liander_electrical_2013.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df3_cast_liander_electricity2013.printSchema()
df3_cast_liander_electricity2014 = df3_liander_electrical_2014.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df3_cast_liander_electricity2014.printSchema()
df3_cast_liander_electricity2015 = df3_liander_electrical_2015.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df3_cast_liander_electricity2015.printSchema()
df3_cast_liander_electricity2016 = df3_liander_electrical_2016.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df3_cast_liander_electricity2016.printSchema()
df3_cast_liander_electricity2017 = df3_liander_electrical_2017.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df3_cast_liander_electricity2017.printSchema()
###
df3_cast_liander_gas2013 = df3_liander_gas_2013.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df3_cast_liander_gas2013.printSchema()
df3_cast_liander_gas2014 = df3_liander_gas_2014.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df3_cast_liander_gas2014.printSchema()
df3_cast_liander_gas2015 = df3_liander_gas_2015.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df3_cast_liander_gas2015.printSchema()
df3_cast_liander_gas2016 = df3_liander_gas_2016.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df3_cast_liander_gas2016.printSchema()
df3_cast_liander_gas2017 = df3_liander_gas_2017.withColumn("net_manager",col("net_manager").cast(StringType()))     .withColumn("purchase_area",col("purchase_area").cast(StringType()))       .withColumn("street",col("street").cast(StringType()))     .withColumn("zipcode_from",col("zipcode_from").cast(StringType()))       .withColumn("zipcode_to",col("zipcode_to").cast(StringType()))     .withColumn("city",col("city").cast(StringType()))      .withColumn("num_connections",col("num_connections").cast(IntegerType()))     .withColumn("delivery_perc",col("delivery_perc").cast(DoubleType()))     .withColumn("perc_of_active_connections",col("perc_of_active_connections").cast(DoubleType()))       .withColumn("type_conn_perc",col("type_conn_perc").cast(IntegerType()))       .withColumn("type_of_connection",col("type_of_connection").cast(StringType()))     .withColumn("annual_consume",col("annual_consume").cast(IntegerType()))     .withColumn("annual_consume_lowtarif_perc",col("annual_consume_lowtarif_perc").cast(DoubleType()))       .withColumn("smartmeter_perc",col("smartmeter_perc").cast(DoubleType()))
df3_cast_liander_gas2017.printSchema()


# In[ ]:


#writing files back to GCS after data exploration

df3_cast_coteq_electricity2013.write.parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_COTEQ/2013', mode='overwrite')
df3_cast_coteq_electricity2014.write.parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_COTEQ/2014', mode='overwrite')
df3_cast_coteq_electricity2015.write.parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_COTEQ/2015', mode='overwrite')
df3_cast_coteq_electricity2016.write.parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_COTEQ/2016', mode='overwrite')
df3_cast_coteq_electricity2017.write.parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_COTEQ/2017', mode='overwrite')
df3_cast_coteq_gas2013.write.parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_COTEQ/2013', mode='overwrite')
df3_cast_coteq_gas2014.write.parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_COTEQ/2014', mode='overwrite')
df3_cast_coteq_gas2015.write.parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_COTEQ/2015', mode='overwrite')
df3_cast_coteq_gas2016.write.parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_COTEQ/2016', mode='overwrite')
df3_cast_coteq_gas2017.write.parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_COTEQ/2017', mode='overwrite')
df3_cast_stedin_electricity2013.write.parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_STEDIN/2013', mode='overwrite')
df3_cast_stedin_electricity2014.write.parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_STEDIN/2014', mode='overwrite')
df3_cast_stedin_electricity2015.write.parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_STEDIN/2015', mode='overwrite')
df3_cast_stedin_electricity2016.write.parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_STEDIN/2016', mode='overwrite')
df3_cast_stedin_electricity2017.write.parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_STEDIN/2017', mode='overwrite')
df3_cast_stedin_gas2013.write.parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_STEDIN/2013', mode='overwrite')
df3_cast_stedin_gas2014.write.parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_STEDIN/2014', mode='overwrite')
df3_cast_stedin_gas2015.write.parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_STEDIN/2015', mode='overwrite')
df3_cast_stedin_gas2016.write.parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_STEDIN/2016', mode='overwrite')
df3_cast_stedin_gas2017.write.parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_STEDIN/2017', mode='overwrite')
df3_cast_enexis_electricity2013.write.parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_ENEXIS/2013', mode='overwrite')
df3_cast_enexis_electricity2014.write.parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_ENEXIS/2014', mode='overwrite')
df3_cast_enexis_electricity2015.write.parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_ENEXIS/2015', mode='overwrite')
df3_cast_enexis_electricity2016.write.parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_ENEXIS/2016', mode='overwrite')
df3_cast_enexis_electricity2017.write.parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_ENEXIS/2017', mode='overwrite')
df3_cast_enexis_gas2013.write.parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_ENEXIS/2013', mode='overwrite')
df3_cast_enexis_gas2014.write.parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_ENEXIS/2014', mode='overwrite')
df3_cast_enexis_gas2015.write.parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_ENEXIS/2015', mode='overwrite')
df3_cast_enexis_gas2016.write.parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_ENEXIS/2016', mode='overwrite')
df3_cast_enexis_gas2017.write.parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_ENEXIS/2017', mode='overwrite')
df3_cast_liander_electricity2013.write.parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_LIANDER/2013', mode='overwrite')
df3_cast_liander_electricity2014.write.parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_LIANDER/2014', mode='overwrite')
df3_cast_liander_electricity2015.write.parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_LIANDER/2015', mode='overwrite')
df3_cast_liander_electricity2016.write.parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_LIANDER/2016', mode='overwrite')
df3_cast_liander_electricity2017.write.parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_LIANDER/2017', mode='overwrite')
df3_cast_liander_gas2013.write.parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_LIANDER/2013', mode='overwrite')
df3_cast_liander_gas2014.write.parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_LIANDER/2014', mode='overwrite')
df3_cast_liander_gas2015.write.parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_LIANDER/2015', mode='overwrite')
df3_cast_liander_gas2016.write.parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_LIANDER/2016', mode='overwrite')
df3_cast_liander_gas2017.write.parquet('gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_LIANDER/2017', mode='overwrite')


