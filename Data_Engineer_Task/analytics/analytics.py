from os import environ
from time import sleep
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from dotenv import load_dotenv
import pandas as pd
import json
from geopy.distance import geodesic

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 30),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'device_data_etl',
    default_args=default_args,
    description='ETL pipeline for device data',
    schedule_interval=timedelta(hours=1),
)

def calculate_distance(location1, location2):
    location1_dict = json.loads(location1)
    location2_dict = json.loads(location2)
    lat1, lon1 = location1_dict['latitude'], location1_dict['longitude']
    lat2, lon2 = location2_dict['latitude'], location2_dict['longitude']
    return geodesic((lat1, lon1), (lat2, lon2)).kilometers

def perform_etl(**kwargs):
    print('ETL Starting...')

    load_dotenv()

    while True:
        try:
            psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
            break
        except OperationalError:
            sleep(0.1)
        sleep(10)
        print(psql_engine)
    print('Connection to PostgresSQL successful.')

    # Write the solution here

    execution_date = kwargs['execution_date']
    start_datetime = execution_date.strftime('%Y-%m-%d %H:%M:%S')
    end_datetime = (execution_date + timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')

    query = text("""
        SELECT 
            device_id, 
            temperature, 
            location, 
            TIMESTAMP 'epoch' + CAST(time AS BIGINT) * INTERVAL '1 second' as time 
        FROM 
            devices 
        WHERE 
            CAST(time AS BIGINT) >= EXTRACT(epoch FROM TIMESTAMP :start_datetime) 
            AND CAST(time AS BIGINT) <= EXTRACT(epoch FROM TIMESTAMP :end_datetime)
        ORDER BY 
            device_id, 
            location,
            TIMESTAMP 'epoch' + CAST(time AS BIGINT) * INTERVAL '1 second' ASC
    """)

    query = query.bindparams(start_datetime=start_datetime, end_datetime=end_datetime)

    df = pd.read_sql_query(query, psql_engine)
    if df.shape[0] ==0:
        return 0
    else:
        df['time'] = df['time'].apply(lambda x: pd.to_datetime(x, unit='s'))
        print(df)
        aggregations = df.groupby('device_id').agg(
            max_temperature=('temperature', 'max'),
            data_points=('temperature', 'count'),
            last_location=('location', 'last'),
            last_time=('time', 'last')).reset_index()

        last_location = None
        total_distances = []
        for _, row in aggregations.iterrows():
            if last_location is None:
                last_location = row['last_location']
                total_distances.append(0)
            else:
                distance = calculate_distance(last_location, row['last_location'])
                total_distances.append(distance)
                last_location = row['last_location']
        aggregations['total_distance'] = total_distances
        print(aggregations)

        mysql_engine = create_engine(environ["MYSQL_CS"], pool_pre_ping=True, pool_size=10)
        with mysql_engine.connect() as mysql_conn:
            aggregations.to_sql('analytics', mysql_conn, if_exists='replace', index=False)
            query = "SELECT * FROM analytics ORDER BY data_points DESC LIMIT 5"
            result = mysql_conn.execute(query)
            rows = result.fetchall()
            df = pd.DataFrame(rows, columns=result.keys())
            print(df)

        print('ETL Completed.')


perform_etl_task = PythonOperator(
    task_id='perform_etl_task',
    python_callable=perform_etl,
    provide_context=True,
    dag=dag,
)

perform_etl_task

if __name__ == "__main__":
    dag.cli()