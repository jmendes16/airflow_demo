import csv
import polars as pl
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook 
from airflow.operators.python import PythonOperator
from tempfile import NamedTemporaryFile
from datetime import datetime, timedelta
from io import StringIO

default_args = {
    'owner': 'airflow_demo',
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

def extract_data():
    db = PostgresHook(postgres_conn_id= 'supabase')
    conn = db.get_conn()
    cursor = conn.cursor()

    timestamp=datetime.now().isoformat()
    cursor.execute('''SELECT * FROM q3sales''')
    with NamedTemporaryFile(
        mode='w',
        suffix=timestamp
        ) as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerows(cursor)
        f.flush()
        cursor.close()
        conn.close()


        s3 = S3Hook(aws_conn_id = 'minio')
        s3.load_file(
            filename=f.name,
            key=f'sales_{timestamp}',
            bucket_name='etl-demo',
            replace=True
        )

def transform_data():
    s3 = S3Hook(aws_conn_id='minio')
    bucket_name = 'etl-demo'
    keys = s3.list_keys(bucket_name)
    if keys:
        keys.sort(reverse=True)
        latest_key = keys[0]
        data = s3.read_key(latest_key, bucket_name)

        data_io = StringIO(data)
        df = pl.read_csv(data_io, has_header=True, separator=',')
        result = df.group_by('product_id').agg(pl.len().alias('count'))
        #df.groupby('product_id').agg(pl.count().alias('count'))

        timestamp=datetime.now().isoformat()
        with NamedTemporaryFile(
            mode='w',
            suffix=timestamp
            ) as f:
            result.write_csv(f.name)
            s3.load_file(
                filename=f.name, 
                key=f'transformed_{timestamp}',
                bucket_name='etl-demo', 
                replace=True
            )
    else:
        raise FileNotFoundError("No files found in bucket.")
        
def load_data():
    s3 = S3Hook(aws_conn_id='minio')
    bucket_name = 'etl-demo'
    keys = s3.list_keys(bucket_name)
    if keys:
        keys.sort(reverse=True)
        latest_key = keys[0]
        data = s3.read_key(latest_key, bucket_name)

        # Establish a connection to the PostgreSQL database
        hook = PostgresHook(postgres_conn_id='fashion_forward_local')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        # SQL to check if the table exists
        cursor.execute(
            "SELECT EXISTS (SELECT FROM information_schema.tables \
                WHERE table_schema = 'public' AND table_name = \
                'q3_product_quantities');"
            )
        table_exists = cursor.fetchone()[0]
        
        # Create the table if it does not exist
        if not table_exists:
            # Constructing a CREATE TABLE statement from dataframe columns
            # This is a simple construction and might need more complex types handling based on your data
            create_table_query = "CREATE TABLE public.q3_product_quantities (product_id VARCHAR(25), count INTEGER);"
            cursor.execute(create_table_query)
            conn.commit()
        
        # Insert data into the PostgreSQL Database
        # We are using the DataFrame's `to_csv` method for simplicity and direct SQL execution for the COPY command
        data_io = StringIO(data)
        data_io.readline() # to get buffer past the headers
        cursor.copy_from(data_io, "q3_product_quantities", sep=',', null='')
        conn.commit()
        
        # Close cursor and connection
        cursor.close()
        conn.close()
    else:
        raise FileNotFoundError("No files found in bucket.")    


with DAG(
    dag_id='ETL_demo',
    default_args=default_args,
    schedule_interval='0 0 * * *', # midnight, everyday
    start_date=days_ago(1),
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data_job',
        python_callable=extract_data
    )

    transform_task = PythonOperator(
        task_id='transform_data_job',
        python_callable=transform_data
    )

    load_task = PythonOperator(
        task_id='load_data_job',
        python_callable=load_data
    )

    extract_task >> transform_task >> load_task