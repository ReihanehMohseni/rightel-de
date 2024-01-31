FROM python:3-slim
#FROM data_engineer_task-analytics
RUN apt-get update && apt-get -y install libpq-dev gcc
RUN pip install psycopg2 sqlalchemy pandas pymysql geopy cryptography python-dotenv apache-airflow datetime
WORKDIR /app
COPY . .
CMD [ "python", "./analytics.py" ]
