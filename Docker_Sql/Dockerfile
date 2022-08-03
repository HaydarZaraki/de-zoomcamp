#Specify which image of python will be used
FROM python:3.9

#Install pandas by default when building the container
RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2 pyarrow

#Specify the Working Directory when building and running the container image
# Equivalant to $cd app
WORKDIR /app

#Copy the file pipeline.py from local to docker image at /app directory
COPY ingestion.py ingestion.py

# ENTRYPOINT is like the environment or commands to run 
# We can use bash or python
# This like executing $python pipeline.py
ENTRYPOINT ["python","ingestion.py"]