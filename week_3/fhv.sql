--Qn.1
-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `data-zoomcamp-338514.trips_data_all.fhv_table_2019`
OPTIONS (
    format = 'parquet',
    uris = ['gs://dtc_data_lake_data-zoomcamp-338514/raw_2/fhv_tripdata_2019-*.parquet']
);

--- performing select on fhv_table_2019
SELECT Count(*) FROM `data-zoomcamp-338514.trips_data_all.fhv_table_2019`;
--- = 42084899

----Qn.2
SELECT COUNT (dispatching_base_num) FROM `data-zoomcamp-338514.trips_data_all.fhv_table_2019`;
--- = 

---Qn.3
-- Create a partitioned table from external table
CREATE OR REPLACE TABLE data-zoomcamp-338514.trips_data_all.fhv_table_2019_partitioned
PARTITION BY
    DATE(dropoff_datetime) AS
SELECT * FROM `data-zoomcamp-338514.trips_data_all.fhv_table_2019`;
