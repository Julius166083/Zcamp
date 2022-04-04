#                                       NETHERLAND ENERGY CONSUMPTION
## Table of contents
   * [Problem statement](https://github.com/Julius166083/Zcamp/blob/master/week_7_project/Readme.md#:~:text=Acknowledgements-,Problem%20statement,-Energy%20is%20at)
   * [Background](https://github.com/Julius166083/Zcamp/blob/master/week_7_project/Readme.md#:~:text=countries/netherlands%2Dpopulation)-,Background,-Background%20about%20the)
   * [Project motivation](https://github.com/Julius166083/Zcamp/blob/master/week_7_project/data_lake_ingestion/dags/README.md#:~:text=the%20zipcode%20ranges-,Project%20motivation,-This%20project%20is)
   * [Methodology](https://github.com/Julius166083/Zcamp/tree/master/week_7_project#:~:text=in%20this%20repository.-,Methodology,-This%20will%20consist)
     * Clone project
     * README links
     * [Technologies](https://github.com/Julius166083/Zcamp/blob/master/week_7_project/data_lake_ingestion/dags/README.md#:~:text=airflow%20DAG%20task-,Technologies,-%3A)
   * [Results](https://github.com/Julius166083/Zcamp/blob/master/week_7_project/data_lake_ingestion/dags/README.md#:~:text=google%20studio%20dashboard-,Results,-Dashboard%20%26%20Documentation)
     * [Dashboard & Documentation](https://github.com/Julius166083/Zcamp/blob/master/week_7_project/data_lake_ingestion/dags/README.md#:~:text=Results-,Dashboard%20%26%20Documentation,-Google%20studio%20tool)
   * [Acknowledgements](https://github.com/Julius166083/Zcamp/blob/master/week_7_project/data_lake_ingestion/dags/README.md#:~:text=Dashboard-,Acknowledgement,-%C2%A9%202022%20GitHub%2C%20Inc)
   
-----------------------------------------------------------------------------------------------------------------------
### Problem statement
Energy is at centre of growth, developement and human survival.The population of Netherlands according to the [worldbank](https://worldpopulationreview.com/countries/netherlands-population) records is expected to keep growing in numbers upto 2034. Its therefore prudent to Understand how Netherland energy consumption is shaping and also know some of the trends of such consumption as it's vital for future planning for utility providers 
         ![image](https://user-images.githubusercontent.com/87927403/161374029-fef220ae-6ce3-4b18-931f-60bd5934819b.png)
         
         
                [source of images](https://worldpopulationreview.com/countries/netherlands-population) 

### Background
Background about the 'Energy (Electricity and Gas) consumption dataset' of the Netherlands, was downloaded from kaggle link address: (https://www.kaggle.com/datasets/lucabasa/dutch-energy).

The energy network of the Netherlands is managed by a few companies. Every year, these companies release on their websites a table with the energy consumption of the areas under their administration. The companies are
       
   * Enexis
   * Liander
   * Stedin
   * Enduris
   * Westlandinfra
   * Rendo
   * Coteq
   
The data are anonymized by aggregating the Zipcodes so that every entry describes at least 10 connections.This market is
not competitive, meaning that the zones are assigned. This means that every year they roughly provide energy to the same zipcodes. Small changes can happen from year to year either for a change of management or for a different aggregation of zipcodes.
       
#### Content
Every file contains information about groups of zipcodes managed by one of the three companies for a specific year.
*NB:* Only for Network administrators of Coteq,Liander, Enexis and Stedin energy companies, were explored in this project.
     
##### Electricity sector
Every file is from a network administrator from a specific year.
The columns in each file are:
       
   * net_manager: code of the regional network manager
   * purchase_area: code of the area where the energy is purchased
   * street: Name of the street
   * zipcode_from and zipcode_to: 2 columns for the range of zipcodes covered, 4 numbers and 2 letters
   * city: Name of the city
   * num_connections: Number of connections in the range of zipcodes
   * delivery_perc: percentage of the net consumption of electricity or gas. The lower, the more energy was given back 
     to the grid (for example if you have solar panels)
   * perc_of_active_connections: Percentage of active connections in the zipcode range
   * type_of_connection: principal type of connection in the zipcode range. For electricity is # fuses X # ampère. For 
     gas is G4, G6, G10, G16, G25
   * type_conn_perc: percentage of presence of the principal type of connection in the zipcode range 
   * annual_consume: Annual consume. Kwh for electricity, m3 for gas
   * annual_consume_lowtarif_perc: Percentage of consume during the low tarif hours. From 10 
     p.m. to 7 a.m. and during weekends.
   * smartmeter_perc: percentage of smartmeters in the zipcode ranges

##### Gas sector
Every file is from a network administrator from a specific year.
The columns in each file are:
       
   * net_manager: code of the regional network manager
   * purchase_area: code of the area where the energy is purchased
   * street: Name of the street
   * zipcode_from and zipcode_to: 2 columns for the range of zipcodes covered, 4 numbers and 2 letters
   * city: Name of the city
   * num_connections: Number of connections in the range of zipcodes
   * delivery_perc: percentage of the net consumption of electricity or gas. The lower, the more energy was given back        to the grid (for example if you have solar panels) 
   * perc_of_active_connections: Percentage of active connections in the zipcode range
   * type_of_connection: principal type of connection in the zipcode range. For electricity is # fuses X # ampère. For        gas is G4, G6, G10, G16, G25    
   * type_conn_perc: percentage of presence of the principal type of connection in the zipcode range  
   * annual_consume: Annual consume. Kwh for electricity, m3 for gas
   * annual_consume_lowtarif_perc: Percentage of consume during the low tarif hours. From 10  p.m. to 7 a.m. and during      weekends.
   * smartmeter_perc: percentage of smartmeters in the zipcode ranges

### Project motivation
This project is part of Data Talk Club by [Alexey Gregory](https://www.youtube.com/watch?v=bkJZDmreIpA&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb), its a Data Engineering nanodegree programme. The data-set [Dutch Energy](https://www.kaggle.com/lucabasa/dutch-energy)is got from a kaggle website. The direction I took is to investigate the data, find insights, trends and information.The findings are published in a report in this repository.
### Methodology
This will consist of workings to show how all set objectives were achieved, details of setups are in separate READMD.md file whose links have been shared here bellow
   * Clone project: use the link shared bellow, then head for "week_7_project" folder to access all details about this project
   
                    git clone https://github.com/Julius166083/Zcamp.git
         
   * README links
      - [Terraform & GCP setup](https://github.com/Julius166083/Zcamp/blob/master/week_7_project/Terraform/README.md)
      - [Spark/Apache-Airflow setup](https://github.com/Julius166083/Zcamp/blob/master/week_7_project/data_lake_ingestion/README.md)
      - [Data lake ingestion](https://github.com/Julius166083/Zcamp/blob/master/week_7_project/data_lake_ingestion/README2.md)   
   * [Dutch-energy.zip](https://www.kaggle.com/lucabasa/dutch-energy) to be downloaded by airflow DAG task 
   * Technologies:Covers only those used here which include:
      * Cloud:Google cloud platform (GCP)
      * containerization: Docker
      * Data analysis and exploartion: Sql & python
      * Infrastructure as code (IaC): Terraform
      * Workflow orchestration: Airflow
      * Data Wareshouse: BigQuery
      * Data ingestion: Batch processing: Spark
      * Visualization: google studio dashboard
### Results
##### Dashboard & Documentation
Google studio tool will be used for visualization
### Acknowledgement
I would like to thank a team of instructors at Data Talk Club (DTC) that have sacrificed time for this community learning off their busy schedules these include:
   * Alexey Grigorev: [linkedin](https://linkedin.com/in/agrigorev) DTC leader
   * Sejal Vaidya: [linkedin](https://linkedin.com/in/vaidyasejal)
   * Ankush Khanna: [linkedin](https://linkedin.com/in/ankushkhanna2)
   * Victoria Perez Mola: [linkedin](https://www.linkedin.com/in/victoriaperezmola/)
      
