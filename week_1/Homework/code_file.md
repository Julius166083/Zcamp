Runing PG_DB:

           docker run -it -e POSTGRES_USER="root" -e POSTGRES_PASSWORD="root" 
          -e POSTGRES_DB="ny_taxi" 
          -v /C/Users/Dell/Desktop/zoomcamp_files/week_1/data_engineer/docker_sql/ny_taxi_postgres_data:/var/lib/postgresql/data  
          -p 5431:5432  --network=pg-network  --name pg-database  postgres:13

Running PGAdmin:

             docker run -it -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" -e PGADMIN_DEFAULT_PASSWORD="root" -p 8080:80
             --network=pg-network --name pgadmin dpage/pgadmin4

Command for running ingest_data pipeline:

URL = "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv" python ingest_data.py --user=root --password=root --host=localhost --port=5432 --db=ny_taxi --table_name=yellow_taxi_trips --url=${URL} Command for running ingest_data inside docker: docker run -it
--network=pg-network
taxi_ingest:v001
--user=root
--password=root
--host=pg-database
--port=5432
--db=ny_taxi
--table_name=yellow_taxi_trips
--url=${URL}

Terraform veriosn =1.1.4

Number of taxi trips in January 15;

          select count(*) from yellow_taxi_trips where date_part('day', tpep_pickup_datetime) = 15; => 53024

Day in January with largest tip;

         select date_part('day', tpep_pickup_datetime) as pickup, max(tip_amount) 
         from yellow_taxi_trips 
         group by pickup
         order by pickup; => 20 Jan

Most popular destination for passengers picked up in central park on January 14;

        SELECT "PULocationID",COUNT("PULocationID") as pu_counts
        FROM yellow_taxi_trips
        WHERE date_part('day', tpep_pickup_datetime) = 14
        GROUP BY "PULocationID"
        ORDER BY pu_counts DESC; => 142: Manhattan Lincoln Square East   yellow Zone (total of 24 passengers)
                                    237: Manhattan Upper East Side South Yellow Zone (total of 24 passengers)
                                    
Pickup-drop-off pair with the largest average price for a ride (calculated based on total amount);

        SELECT "PULocationID","DOLocationID",AVG(total_amount) as max_amount
        FROM yellow_taxi_trips
        GROUP BY "PULocationID", "DOLocationID"
        ORDER BY max_amount DESC; => 234 and 39 (Union Sq and Canarsie)