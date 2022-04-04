

---------------------------------------------------------------------------------------------
------------------------------ Electrical ENERGY CONSUMPTION----------------------------------------
---------------------------------------------------------------------------------------------


--- Creating external table "external_electricity_consumption_table" referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `data-zoomcamp-338514.energy_consumption.external_electricity_consumption_table`
OPTIONS (
  format = 'parquet',
  uris = [
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_COTEQ/2013/part-00000-39b15dab-726e-4a21-8f92-5d97fbbcabc9-c000.snappy.parquet', 
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_COTEQ/2014/part-00000-7ae31b80-19ba-489a-b8be-1360ab4e1098-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_COTEQ/2015/part-00000-f4f97854-1e7f-45b7-8cc4-3f1b4cedc223-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_COTEQ/2016/part-00000-c0bd7836-06b4-444f-bf7a-d480f6adfabc-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_COTEQ/2017/part-00000-1b90ac3e-d9bd-47dc-ae97-faefef7b26a0-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_ENEXIS/2013/part-00000-8f8f96ed-72b1-498d-9b0d-9f4bb4c2dc30-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_ENEXIS/2014/part-00000-0052b977-1890-44d0-afc4-7d21371ceff4-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_ENEXIS/2015/part-00000-f1178d99-217c-49ae-b35f-179617508f5a-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_ENEXIS/2016/part-00001-21d451c3-8cdc-4d89-8a71-4a14ce7009aa-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_ENEXIS/2016/part-00003-21d451c3-8cdc-4d89-8a71-4a14ce7009aa-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_LIANDER/2013/part-00000-3a9c01f9-3a65-40d1-9d25-a6a5999fdbf1-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_LIANDER/2014/part-00000-3ad12277-445d-4cc5-bbc2-05f8b182b938-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_LIANDER/2015/part-00000-d4ffac1b-f897-4d7b-8351-1eb28a6954e5-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_LIANDER/2016/part-00000-235f4d42-8fd4-4d08-b6e8-c275ff312603-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_LIANDER/2017/part-00000-d8f7810f-d540-4746-81af-01817a138240-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_STEDIN/2013/part-00000-e6e97b30-38f8-4094-91c2-bb4545336149-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_STEDIN/2014/part-00000-46a539f4-6ad5-4a2f-b4cf-2111c1bfa708-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_STEDIN/2015/part-00000-eb981599-2257-4ba4-ae3b-34165c81e4a2-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_STEDIN/2016/part-00000-e32f762b-2bdb-4a48-a486-9d03046dd434-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_STEDIN/2017/part-00000-62470c63-b0ee-4e9b-bc44-3e2feee1d180-c000.snappy.parquet'

      ]
);


---------------------------------------------------------------------------------------------
------------------------------ GAS ENERGY CONSUMPTION----------------------------------------
---------------------------------------------------------------------------------------------

--- Creating external table "external_gas_consumption_table" referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `data-zoomcamp-338514.energy_consumption.external_gas_consumption_table`
OPTIONS (
  format = 'parquet',
  uris = [
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_COTEQ/2013/part-00000-4c61c9cb-1453-4f78-a9a8-116607b23a6b-c000.snappy.parquet', 
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_COTEQ/2014/part-00000-3f2b87b2-43a1-442b-8fd8-97d96886fc5e-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_COTEQ/2015/part-00000-47659be4-96fa-435d-b057-89f4daff69d8-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_COTEQ/2016/part-00000-90692c6b-4b22-4708-aa41-3326cd2c3c58-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_COTEQ/2017/part-00000-cc83e2f7-70f2-434d-acff-b2bb2170104b-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_ENEXIS/2013/part-00000-c285b269-9435-49cc-8cde-3c0b2b0d5080-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_ENEXIS/2014/part-00000-3779a21c-f131-4e76-87d3-e0fba7f9a85b-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_ENEXIS/2015/part-00000-dcc1af15-cbe8-4fab-91dc-9a8d26de964b-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_ENEXIS/2016/part-00000-6f0fd92a-4553-441f-a589-6f3f84579e49-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_ENEXIS/2017/part-00000-b1cc2ba0-5d62-46b9-b0ca-79033528b035-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_LIANDER/2013/part-00000-0ba434d4-9c32-4d36-aaf9-f2ea8953feb7-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_LIANDER/2014/part-00000-076b2137-5ac6-4aba-9d38-f17b498a3885-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_LIANDER/2015/part-00000-48fe7131-4b56-42e2-9fac-f57083be8e8d-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_LIANDER/2016/part-00000-acc85cf0-c97e-44c0-a819-84644e213ac6-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_LIANDER/2017/part-00001-39cb9ca7-7063-4a8e-a68d-e405f382ca61-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_STEDIN/2013/part-00000-4fe0a79c-f185-40aa-b843-32c902408986-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_STEDIN/2014/part-00000-269d9f17-6a0c-4b19-98dd-a0e85561252d-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_STEDIN/2015/part-00000-3ea54f3d-bad6-463a-967a-a7c57bb632fd-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_STEDIN/2016/part-00000-d5ea0aee-a5c2-4195-933e-a4e9eb075614-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_STEDIN/2017/part-00000-9c9b5dd5-9305-434d-985c-64b922add147-c000.snappy.parquet'
      ]
);


SELECT * FROM `data-zoomcamp-338514.energy_consumption.external_gas_consumption_table`;


SELECT net_manager, num_connections FROM `data-zoomcamp-338514.energy_consumption.external_gas_consumption_table`
ORDER BY num_connections;

SELECT net_manager, num_connections, city FROM `data-zoomcamp-338514.energy_consumption.external_gas_consumption_table`
ORDER BY city;


---------------------------------------------------------------------------------------------
------------------------------ combined Electrical & Gas CONSUMPTION----------------------------------------
---------------------------------------------------------------------------------------------

-- Creating external table "combined-gas& electricity" referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `data-zoomcamp-338514.energy_consumption.external_General_consumption_table`
OPTIONS (
  format = 'parquet',
  uris = [

      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_COTEQ/2013/part-00000-39b15dab-726e-4a21-8f92-5d97fbbcabc9-c000.snappy.parquet', 
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_COTEQ/2014/part-00000-7ae31b80-19ba-489a-b8be-1360ab4e1098-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_COTEQ/2015/part-00000-f4f97854-1e7f-45b7-8cc4-3f1b4cedc223-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_COTEQ/2016/part-00000-c0bd7836-06b4-444f-bf7a-d480f6adfabc-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_COTEQ/2017/part-00000-1b90ac3e-d9bd-47dc-ae97-faefef7b26a0-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_ENEXIS/2013/part-00000-8f8f96ed-72b1-498d-9b0d-9f4bb4c2dc30-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_ENEXIS/2014/part-00000-0052b977-1890-44d0-afc4-7d21371ceff4-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_ENEXIS/2015/part-00000-f1178d99-217c-49ae-b35f-179617508f5a-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_ENEXIS/2016/part-00001-21d451c3-8cdc-4d89-8a71-4a14ce7009aa-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_ENEXIS/2016/part-00003-21d451c3-8cdc-4d89-8a71-4a14ce7009aa-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_LIANDER/2013/part-00000-3a9c01f9-3a65-40d1-9d25-a6a5999fdbf1-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_LIANDER/2014/part-00000-3ad12277-445d-4cc5-bbc2-05f8b182b938-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_LIANDER/2015/part-00000-d4ffac1b-f897-4d7b-8351-1eb28a6954e5-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_LIANDER/2016/part-00000-235f4d42-8fd4-4d08-b6e8-c275ff312603-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_LIANDER/2017/part-00000-d8f7810f-d540-4746-81af-01817a138240-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_STEDIN/2013/part-00000-e6e97b30-38f8-4094-91c2-bb4545336149-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_STEDIN/2014/part-00000-46a539f4-6ad5-4a2f-b4cf-2111c1bfa708-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_STEDIN/2015/part-00000-eb981599-2257-4ba4-ae3b-34165c81e4a2-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_STEDIN/2016/part-00000-e32f762b-2bdb-4a48-a486-9d03046dd434-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/ELECTRICITY_STEDIN/2017/part-00000-62470c63-b0ee-4e9b-bc44-3e2feee1d180-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_COTEQ/2013/part-00000-4c61c9cb-1453-4f78-a9a8-116607b23a6b-c000.snappy.parquet', 
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_COTEQ/2014/part-00000-3f2b87b2-43a1-442b-8fd8-97d96886fc5e-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_COTEQ/2015/part-00000-47659be4-96fa-435d-b057-89f4daff69d8-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_COTEQ/2016/part-00000-90692c6b-4b22-4708-aa41-3326cd2c3c58-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_COTEQ/2017/part-00000-cc83e2f7-70f2-434d-acff-b2bb2170104b-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_ENEXIS/2013/part-00000-c285b269-9435-49cc-8cde-3c0b2b0d5080-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_ENEXIS/2014/part-00000-3779a21c-f131-4e76-87d3-e0fba7f9a85b-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_ENEXIS/2015/part-00000-dcc1af15-cbe8-4fab-91dc-9a8d26de964b-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_ENEXIS/2016/part-00000-6f0fd92a-4553-441f-a589-6f3f84579e49-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_ENEXIS/2017/part-00000-b1cc2ba0-5d62-46b9-b0ca-79033528b035-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_LIANDER/2013/part-00000-0ba434d4-9c32-4d36-aaf9-f2ea8953feb7-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_LIANDER/2014/part-00000-076b2137-5ac6-4aba-9d38-f17b498a3885-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_LIANDER/2015/part-00000-48fe7131-4b56-42e2-9fac-f57083be8e8d-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_LIANDER/2016/part-00000-acc85cf0-c97e-44c0-a819-84644e213ac6-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_LIANDER/2017/part-00001-39cb9ca7-7063-4a8e-a68d-e405f382ca61-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_STEDIN/2013/part-00000-4fe0a79c-f185-40aa-b843-32c902408986-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_STEDIN/2014/part-00000-269d9f17-6a0c-4b19-98dd-a0e85561252d-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_STEDIN/2015/part-00000-3ea54f3d-bad6-463a-967a-a7c57bb632fd-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_STEDIN/2016/part-00000-d5ea0aee-a5c2-4195-933e-a4e9eb075614-c000.snappy.parquet',
      'gs://data_lake_week_7_project_data-zoomcamp-338514/PROCESSED/GAS_STEDIN/2017/part-00000-9c9b5dd5-9305-434d-985c-64b922add147-c000.snappy.parquet'
      ]
);


SELECT * FROM `data-zoomcamp-338514.energy_consumption.external_General_consumption_table`;


SELECT net_manager, num_connections FROM `data-zoomcamp-338514.energy_consumption.external_General_consumption_table`
ORDER BY num_connections;

SELECT net_manager, num_connections, city FROM `data-zoomcamp-338514.energy_consumption.external_General_consumption_table`
ORDER BY city;




