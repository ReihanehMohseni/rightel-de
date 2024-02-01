from os import environ
from time import sleep
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from dotenv import load_dotenv
import pandas as pd

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

query = text("SELECT device_id, temperature, location, time FROM devices")
df = pd.read_sql_query(query, psql_engine)

aggregations = df.groupby('device_id').agg(
    max_temperature=('temperature', 'max'),
    data_points=('temperature', 'count'),
    last_location=('location', 'last'),
    last_time=('time', 'last')
).reset_index()

print(aggregations)