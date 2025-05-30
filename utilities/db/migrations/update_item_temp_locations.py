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
update_sql = "update item set temp_location_id = %(temp_location_id)s where pid = %(pid)s"
cursor.itersize = 1000
cursor.execute(sql, None)

cnt = 0
start_time = time.time()
location_id_dict = {}


update_cursor.execute('select id, library_code, code from location')
while True:
    records = update_cursor.fetchmany(1000)
    if not records:
        break
    for row in records:
        location_id_dict[(row[1], row[2])] = row[0]

def lookup_location(location):
    return location_id_dict.get(location, None)

while True:
    records = cursor.fetchmany(1000)
    if not records:
        break
    for record in records:
        json_obj = json.loads(record[0])
        in_temp = json_obj['holding_data'].get('in_temp_location', False)
        if in_temp:
            temp_library_code = json_obj['holding_data'].get('temp_library', {}).get('value', None) if in_temp else None
            temp_location_code = json_obj['holding_data'].get('temp_location', {}).get('value', None) if in_temp else None
            if temp_library_code and temp_library_code:
                temp_location_id = lookup_location((temp_library_code, temp_location_code))
                update_cursor.execute(update_sql, {"pid": record[1], "temp_location_id": temp_location_id})
                cnt += 1
                print(f"updated {record[1]}")
                if cnt % 1000 == 0:
                    connection.commit();
                    print(f"processed {cnt} at {cnt / (time.time() - start_time)} records per second")
connection.commit()
print(f"processed {cnt} at {cnt / (time.time() - start_time)} records per second")
connection.close()
connection2.close()