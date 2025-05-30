import psycopg2
import os
import json
import time


connection = psycopg2.connect(
                user=os.getenv("DB_USERNAME"),
                password=os.getenv("DB_PASSWORD"),
                host=os.getenv("DB_HOST"),
                port="5432",
                database=os.getenv("DB_SCHEMA"),
            )

connection2 = psycopg2.connect(
                user=os.getenv("DB_USERNAME"),
                password=os.getenv("DB_PASSWORD"),
                host=os.getenv("DB_HOST"),
                port="5432",
                database=os.getenv("DB_SCHEMA"),
            )
cursor = connection2.cursor(name="serverside_cursor")

update_cursor = connection.cursor()

sql = "select data, pid from item_data"
update_sql = "update item set policy = %(policy)s where pid = %(pid)s"
cursor.itersize = 1000
cursor.execute(sql, None)

cnt = 0
start_time = time.time()

while True:
    records = cursor.fetchmany(1000)
    if not records:
        break
    for record in records:
        dt = json.loads(record[0])
        policy = dt['item_data'].get('policy', {}).get('value', '')
        if policy:
            update_cursor.execute(update_sql, {"pid": record[1], "policy": policy})
            cnt += 1
            if cnt % 1000 == 0:
                connection.commit();
                print(f"processed {cnt} at {cnt / (time.time() - start_time)} records per second")
connection.commit()
print(f"processed {cnt} at {cnt / (time.time() - start_time)} records per second")
connection.close()
connection2.close()