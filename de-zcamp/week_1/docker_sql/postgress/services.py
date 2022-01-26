services:
    postgress:
        image: postgress:13
        environment:
            POSTGRESS_USER: airflow
            POSTGRESS_PASSWORD:airflow
            POSTGRESS_DB:airflow
        volume:
            - postgres_db-volume:/var/lib/postgresql/data
        healthcheck:
            test:["CMD", "pg_isready", "-U", "airflow"]
            interval: 5s
            retries:5
        restart: always
        

# yellow_taxi_database into pstgres
    docker run -it \  
        -e POSTGRES_USER="root" \ 
        -e POSTGRES_PASSWORD="root" \  
        -e POSTGRES_DB="ny_taxi" \ 
        -v /C/zoomcamp/data_engineer/week_1_fundamentals/2_docker_sql/ny_taxi_postgres_data:/var/lib/postgresql/data \ 
        -p 5431:5432 \
         --network=pg-network \
         --name pg-database \
        postgres:13


# yellow_taxi_database into pstgres
docker run -it \  
        -e POSTGRES_USER="root" \ 
        -e POSTGRES_PASSWORD="root" \  
        -e POSTGRES_DB="taxi_zone" \ 
        -v /C/zoomcamp/data_engineer/week_1_fundamentals/2_docker_sql/ny_taxi_postgres_data:/var/lib/postgresql/data \ 
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









  