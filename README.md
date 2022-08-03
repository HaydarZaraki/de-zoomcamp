# de-zoomcamp
This Repository is a follow-along for the data Engineering Zoomcamp from DataTalks Club

Useful Commands :

- Running Postgreql container:
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  postgres:13
  
- Running pgcli:
pgcli -h localhost -p 5432 -u root -d ny_taxi

  
- Running PgAdmin4:
docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
  dpage/pgadmin4
  
- Create Network on Docker
docker network create pg-network

Docker:
- We need docker to be installed with python 3.9
- Create Dockerfile for instructions on building image
- Create pipeline.py to be copied when starting the container and then execute it
