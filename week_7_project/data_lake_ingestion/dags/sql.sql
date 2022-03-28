
--- Creating external table "external_electricity_consumption_table" referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `data-zoomcamp-338514.energy_consumption.external_electricity_consumption_table`
OPTIONS (
  format = 'parquet',
  uris = [
      'gs://data_lake_week_7_project_data-zoomcamp-338514/ELECTRICITY_COTEQ/coteq_electricity_*.parquet', 
      'gs://data_lake_week_7_project_data-zoomcamp-338514/ELECTRICITY_ENDINET/endinet_electricity_*.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/ELECTRICITY_ENDURIS/enduris_electricity_*.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/ELECTRICITY_ENEXIS/enexis_electricity_*.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/ELECTRICITY_LIANDER/liander_electricity_*.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/ELECTRICITY_RENDO/rendo_electricity_*.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/ELECTRICITY_STEDIN/stedin_electricity_*.parquet'
      ]
);

-- Checking 'external_electricity_consumption_table' data
SELECT * FROM `data-zoomcamp-338514.energy_consumption.external_electricity_consumption_table` limit 10;

-- Create a non partitioned table from external table of 'external_electricity_consumption_table'
CREATE OR REPLACE TABLE 'data-zoomcamp-338514.energy_consumption.electricity_consumption_non_partitioned_table' AS
SELECT * FROM `data-zoomcamp-338514.energy_consumption.external_electricity_consumption_table`;


-- Create a partitioned table from external table
CREATE OR REPLACE TABLE 'data-zoomcamp-338514.energy_consumption.electricity_consumption_partitioned_table'
PARTITION BY num_connections AS
SELECT * FROM `data-zoomcamp-338514.energy_consumption.external_electricity_consumption_table`;



---------------------------------------------------------------------------------------------
------------------------------ GAS ENERGY CONSUMPTION----------------------------------------
---------------------------------------------------------------------------------------------

--- Creating external table "external_gas_consumption_table" referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `data-zoomcamp-338514.energy_consumption.external_gas_consumption_table`
OPTIONS (
  format = 'parquet',
  uris = [
      'gs://data_lake_week_7_project_data-zoomcamp-338514/GAS_COTEQ/coteq_gas_*.parquet', 
      'gs://data_lake_week_7_project_data-zoomcamp-338514/GAS_ENDINET/endinet_gas_*.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/GAS_ENDURIS/enduris_gas_*.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/GAS_ENEXIS/enexis_gas_*.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/GAS_LIANDER/liander_gas_*.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/GAS_RENDO/rendo_gas_*.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/GAS_STEDIN/stedin_gas_*.parquet'
      ]
);