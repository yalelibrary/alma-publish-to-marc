#
#  Read IDs from marc files
#
import logging
import psycopg2
import os

logger = logging.getLogger(__name__)

def mmsids_from_database():
    connection = psycopg2.connect(
        user=os.getenv("DB_USERNAME"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port="5432",
        database=os.getenv("DB_SCHEMA"),
    )
    cursor = connection.cursor("server-side")

    cursor.itersize = 1000
    cursor.execute('select mms_id from bib_brief')

    while True:
        records = cursor.fetchmany(1000)
        if not records:
            break
        for row in records:
            print(row[0])

    connection.close()

def main():
    mmsids_from_database()

if __name__ == '__main__':
    main()

