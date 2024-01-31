from os import environ
from time import sleep
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from dotenv import load_dotenv

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

with psql_engine.connect() as conn:
    query = text("SELECT device_id, temperature, location, time FROM devices;")
    result = conn.execute(query)
    rows = result.fetchall()
    print(rows)