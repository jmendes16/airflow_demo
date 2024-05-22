To use the code in this demo you will need docker installed on your device.  
Instructions and download here https://www.docker.com/products/docker-desktop/

The original compose file can be found here:
https://airflow.apache.org/docs/apache-airflow/2.9.0/docker-compose.yaml

You will also need the relevant directories within project root directory 
Run this command to create them
mkdir -p ./logs ./plugins ./config /data

In your python environment you will need to run
pip install airflow 
pip install -r requirements.txt

Also in the project root directory you will need to run
docker build . --tag extending_airflow:latest
This builds the airflow image used in the yaml file

To launch all the services you will need to run 
docker compose up -d
Which launches all services in detached mode

In Airflow you UI (found at localhost:8080) you can edit the connection settings 
for for supabase, minio and your local postgres server
Make sure your supabase instance is setup with the fashion forward database as it 
appears in the SQL Ramp Up module.  You will also need to create a local database
named 'fashion_forward'.  

In the minio UI you need to create a bucket called etl-demo.