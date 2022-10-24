from data_diff import connect_to_table, diff_tables
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from pathlib import Path
import logging
import os
import time

if Path(".env").is_file():
    load_dotenv()

GOOGLE_KEY_PATH = "gcloud_key_tzanakis-bigquery.json"
MYSQL_TABLE = "reservations_bookings"
BQ_DATASET = "user_gtzanakis"
ID_MIN = 2000000
ID_MAX = 2103353

logging.basicConfig(level=logging.INFO)


# Configure MySQL

# Alternatively use uri:
# db_info = f"mysql://{user}:{password}@{hostname}/{database}"

db_info_mysql = {
    "driver": "mysql",
    "user": os.environ["MYSQL_USER"],
    "password": os.environ["MYSQL_PASSWORD"],
    "database": os.environ["MYSQL_DATABASE"],
    "host": os.environ["MYSQL_HOST"],
}


# Configure BigQuery

credentials_gcloud = Credentials.from_service_account_file(
    filename=GOOGLE_KEY_PATH, scopes=["https://www.googleapis.com/auth/bigquery"]
)

db_info_bigquery = {
    "driver": "bigquery",
    "location": "eu",
    "credentials": credentials_gcloud,
    "project": os.environ["BQ_PROJECT_ID"],
    "dataset": BQ_DATASET,
}

table_mysql_bookings = connect_to_table(
    db_info_mysql,
    table_name="reservations_bookings",
    key_column="id",
    min_key=ID_MIN,
    max_key=ID_MAX,
    where="bookingState='BOOKED'",
)

table_bigquery = connect_to_table(
    db_info_bigquery,
    table_name="booking_ids",
    key_column="id",
    min_key=ID_MIN,
    max_key=ID_MAX,
)


def main():

    start = time.time()

    for different_row in diff_tables(table_mysql_bookings, table_bigquery):
        plus_or_minus, columns = different_row
        print(plus_or_minus, columns)

    print(time.time() - start)


if __name__ == "__main__":
    main()
