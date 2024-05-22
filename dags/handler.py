from tempfile import NamedTemporaryFile
from csv import writer
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook 
from psycopg2.extensions import cursor
from datetime import datetime
from io import StringIO

def get_database_cursor(connection_id: str) -> tuple:
    '''returns connection and cursor object'''
    db = PostgresHook(postgres_conn_id= connection_id)
    conn = db.get_conn()
    return conn, conn.cursor()

def create_csv(file: NamedTemporaryFile, query_result: cursor) -> writer:
    '''returns csv from database query'''
    csv = writer(file)
    csv.writerow([i[0] for i in query_result.description])
    csv.writerows(query_result)
    return csv

def load_to_bucket(connection_id: str, file: NamedTemporaryFile, filename: str) -> None:
    s3 = S3Hook(aws_conn_id = connection_id)
    s3.load_file(
        filename=file.name,
        key=filename + '_' + str(datetime.now().isoformat()),
        bucket_name='etl-demo',
        replace=True
    )

def bucket_empty(connection_id: str) -> bool:
    s3 = S3Hook(aws_conn_id=connection_id)
    return not bool(s3.list_keys('etl-demo'))

def get_most_recent_data(connection_id: str) -> StringIO:
    # create connection to bucket
    s3 = S3Hook(aws_conn_id=connection_id)
    
    # order keys to get latest
    keys = s3.list_keys('etl-demo')
    keys.sort(reverse=True)
    latest_key = keys[0]
    print(latest_key)
    
    # read data
    data = s3.read_key(latest_key, 'etl-demo')

    return StringIO(data)

def destination_table_exists(destination_cursor: cursor) -> bool:
    #SQL to check if the table exists
    destination_cursor.execute(
        "SELECT EXISTS (SELECT FROM information_schema.tables \
            WHERE table_schema = 'public' AND table_name = \
            'q3_product_quantities');"
        )
    return bool(destination_cursor.fetchone()[0])