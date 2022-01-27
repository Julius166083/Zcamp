services:
        

    # yellow_taxi_trip_DB into pstgres
    docker run -it \  
        -e POSTGRES_USER="root" \ 
        -e POSTGRES_PASSWORD="root" \  
        -e POSTGRES_DB="ny_taxi" \ 
        -v /C/Users/Dell/Desktop/DTZC/DE/DTZCamp/Zcamp/week_1/docker_sql/postgress/ny_taxi_postgres_data:/var/lib/postgresql/data \ 
        -p 5431:5432 \
        --network= pg-network \
        --name pg-database \
        postgres:13


    # yellow_taxi_zones_DB into pstgres
    docker run -it \  
        -e POSTGRES_USER="root" \ 
        -e POSTGRES_PASSWORD="root" \  
        -e POSTGRES_DB="taxi_zone" \ 
        -v /C/Users/Dell/Desktop/DTZC/DE/DTZCamp/Zcamp/week_1/docker_sql/postgress/ny_taxi_postgres_data:/var/lib/postgresql/data \ 
        -p 5431:5432 \
        --network=pg-network \
        --name pg-database \
        postgres:13
        
    #pgadmin details
    docker run -it \
        -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
        -e PGADMIN_DEFAULT_PASSWORD="root" \
        -p 8080:80 \
        --network=pg-network \
        --name pgadmin \
        dpage/pgadmin4




    URL="https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv"

    python yellow_taxi_tripdataSCRIPT.py \
        --user=root \
        --password=root \
        --host=localhost \
        --port=5431 \
        --db=ny_taxi \
        --table_name=yellow_taxi_data_table \
        --url=${URL}



# build the image table
docker build -t taxi_ingest_image:v001 .


docker run -it \
    --network=docker_compose_default  \
    taxi_ingest_image:v001 \
        --user=root \
        --password=root \
        --host=pgdatabase \
        --port=5431 \
        --db=ny_taxi \
        --table_name=yellow_taxi_data_table \
        --url=${URL}
    