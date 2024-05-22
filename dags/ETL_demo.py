import polars as pl
from handler import *
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from tempfile import NamedTemporaryFile
from datetime import  timedelta

default_args = {
    'owner': 'airflow_demo',
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

def extract_data():
    # getting the raw data from data source
    conn, cursor = get_database_cursor('supabase')
    cursor.execute('''SELECT * FROM q3sales''')

    # creating the csv
    with NamedTemporaryFile(
        mode='w',
        suffix='raw'
        ) as f:
        csv = create_csv(f,cursor)
        f.flush()
        cursor.close()
        conn.close()

        # loading the data to S3 bucket
        load_to_bucket('minio', f, 'sales')

def transform_data():
    if bucket_empty('minio'):
        raise FileNotFoundError("No files found in bucket.")
    else:
        data_io = get_most_recent_data('minio')
        
        # read data into polars dataframe and transform it
        df = pl.read_csv(data_io, has_header=True, separator=',')
        result = df.group_by('product_id').agg(pl.len().alias('count'))

        # creating the csv
        with NamedTemporaryFile(
            mode='w',
            suffix='transformed'
            ) as f:
            result.write_csv(f.name)

            # loading the data to S3 bucket
            load_to_bucket('minio', f, 'transformed')
        
def load_data():
    if bucket_empty('minio'):
        raise FileNotFoundError("No files found in bucket.")
    else:
        data_io = get_most_recent_data('minio')

        conn, cursor = get_database_cursor('fashion_forward_local')

        if not destination_table_exists(cursor):
            # Constructing a CREATE TABLE statement from dataframe columns
            create_table_query = "CREATE TABLE public.q3_product_quantities (product_id VARCHAR(25), count INTEGER);"
            cursor.execute(create_table_query)
            conn.commit()
        
        data_io.readline() # to get buffer past the headers
        # uses the COPY SQL command to get data into relevant table
        cursor.copy_from(data_io, "q3_product_quantities", sep=',', null='')
        conn.commit()
        
        # Close cursor and connection
        cursor.close()
        conn.close()
    
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