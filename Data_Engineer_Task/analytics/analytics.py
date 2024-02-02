from os import environ
from time import sleep
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from dotenv import load_dotenv
import pandas as pd
import json
from geopy.distance import geodesic

load_dotenv()

print('Waiting for the data generator...')
sleep(2)
print('ETL Starting...')

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

def calculate_distance(location1, location2):
    location1_dict = json.loads(location1)
    location2_dict = json.loads(location2)
    lat1, lon1 = location1_dict['latitude'], location1_dict['longitude']
    lat2, lon2 = location2_dict['latitude'], location2_dict['longitude']
    return geodesic((lat1, lon1), (lat2, lon2)).kilometers

start_datetime = "2024-01-31 18:00:00"
end_datetime = "2024-01-31 18:59:59"

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
    aggregations.to_sql('aggregated_data', mysql_conn, if_exists='replace', index=False)

print('ETL Completed.')