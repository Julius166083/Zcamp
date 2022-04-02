##                                       NETHERLAND ENERGY
### Table of contents
      [Background](https://github.com/Julius166083/Zcamp/blob/master/week_7_project/data_lake_ingestion/dags/README.md#:~:text=Results%0A%20%20*%20Author%0A%20%20*%20Thanks-,Background,-**%20Background%20about%20the)
      * Project motivation
      * File descriptions
      * Results
      * Author
      * Thanks
-----------------------------------------------------------------------------------------------------------------------

### Background
    ** Background about the 'Energy (Electricity and Gas) consumption dataset' of the Netherlands, dataset downloaded
       from kaggle API at link address: (https://www.kaggle.com/datasets/lucabasa/dutch-energy).

       The energy network of the Netherlands is managed by a few companies. Every year, these companies release on their websites a table with the energy consumption of the areas under their administration. The companies are
       
       * Enexis
       * Liander
       * Stedin
       * Enduris
       * Westlandinfra
       * Rendo
       * Coteq
       The data are anonymized by aggregating the Zipcodes so that every entry describes at least 10 connections.
       
       This market is not competitive, meaning that the zones are assigned. This means that every year they roughly provide energy to the same zipcodes. Small changes can happen from year to year either for a change of management or for a different aggregation of zipcodes.
       
       Content
       Every file contains information about groups of zipcodes managed by one of the three companies for a specific year.
     NB: company of Westlandinfra was not considered in this project exploration by choice.
     
####      About the 'Electricity' directory

       Every file is from a network administrator from a specific year.
       The columns in each file are:
       
       * net_manager: code of the regional network manager
       * purchase_area: code of the area where the energy is purchased
       * street: Name of the street
       * zipcode_from and zipcode_to: 2 columns for the range of zipcodes covered, 4 numbers
         and 2 letters
       * city: Name of the city
       * num_connections: Number of connections in the range of zipcodes
       * delivery_perc: percentage of the net consumption of electricity or gas. The lower, the
         more energy was given back to the grid (for example if you have solar panels)
       * perc_of_active_connections: Percentage of active connections in the zipcode range
       * type_of_connection: principal type of connection in the zipcode range. For electricity 
         is # fuses X # ampère. For gas is G4, G6, G10, G16, G25
       * type_conn_perc: percentage of presence of the principal type of connection in the 
         zipcode range
       * annual_consume: Annual consume. Kwh for electricity, m3 for gas
       * annual_consume_lowtarif_perc: Percentage of consume during the low tarif hours. From 10 
         p.m. to 7 a.m. and during weekends.
       * smartmeter_perc: percentage of smartmeters in the zipcode ranges

####      About the 'Gas' directory

       Every file is from a network administrator from a specific year.
       The columns in each file are:
       
       * net_manager: code of the regional network manager
       * purchase_area: code of the area where the energy is purchased
       * street: Name of the street
       * zipcode_from and zipcode_to: 2 columns for the range of zipcodes covered, 4 numbers and
         2 letters
       * city: Name of the city
       * num_connections: Number of connections in the range of zipcodes
       * delivery_perc: percentage of the net consumption of electricity or gas. The lower, the 
         more energy was given back to the grid (for example if you have solar panels)
       * perc_of_active_connections: Percentage of active connections in the zipcode range
       * type_of_connection: principal type of connection in the zipcode range. For electricity 
         is # fuses X # ampère. For gas is G4, G6, G10, G16, G25
       * type_conn_perc: percentage of presence of the principal type of connection in the 
         zipcode range
       * annual_consume: Annual consume. Kwh for electricity, m3 for gas
       * annual_consume_lowtarif_perc: Percentage of consume during the low tarif hours. From 10
         p.m. to 7 a.m. and during weekends.
       * smartmeter_perc: percentage of smartmeters in the zipcode ranges
         
