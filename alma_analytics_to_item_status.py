import argparse
import csv
import logging
import os
from pathlib import Path
import time
from database_insert import StatementExecutor, generate_insert_prepared_statements
logger = logging.getLogger(__name__)

def prepare_status_statement(database):
    sql = generate_insert_prepared_statements('item_base_status_insert', 'item_base_status', ['pid', 'due_date', 'status_code', 'process_type', 'process_status', 'renewal_date', 'loan_date' ], 1)
    item_base_status_insert = sql['execute_statement']
    database.execute_statement(sql['prepared_statement'])
    return item_base_status_insert;

def process_row(database, row, item_base_status_insert):
    if row[3] == 'Active' and row[2] == 'Item not in place':
        pid = row[0]
        due_date = row[1] or None
        status_code = '0'
        process_type = row[4].upper() or None
        process_status = row[5] or None
        renewal_date = row[6] or None
        loan_date = row[7] or None
        params = {'pid': pid, 'due_date': due_date, 'status_code': status_code, 'process_type': process_type, 'process_status': process_status, 'renewal_date': renewal_date, 'loan_date': loan_date}
        database.execute_statement(item_base_status_insert, params)
        logger.debug(f'updating {pid}')
    else:
        logger.info(f'Row without Active or Item not in place: ${row}')

def import_file(filename):
    cnt = 0
    with StatementExecutor() as database:
        item_base_status_insert = prepare_status_statement(database)
        prior_row = None
        prior_row_pid = None
        with open(filename, 'r', encoding='utf-16') as tsvfile:
            reader = csv.reader(tsvfile, delimiter='\t')
            for row in reader:
                if row[2] == 'Item not in place':
                    if prior_row_pid != row[0] and prior_row:
                        process_row(database, prior_row, item_base_status_insert)
                        cnt += 1
                        if cnt % 1000 == 0:
                            database.commit()
                            logger.info(f'Processed {cnt} records')
                    prior_row_pid = row[0];
                    prior_row = row
        if prior_row:
            process_row(database, prior_row, item_base_status_insert)
            cnt += 1
            database.commit()
    return cnt


def configure_logging(name):
    root_logger = logging.getLogger()
    log_level = os.getenv("LOG_LEVEL", "INFO")
    root_logger.setLevel(log_level)
    console_handler = logging.StreamHandler()
    root_logger.addHandler(console_handler)
    fh_info = logging.FileHandler(name)
    fh_info.setLevel(log_level)
    root_logger.addHandler(fh_info)


def main():
    configure_logging(f'{Path(__file__).stem}.log')
    logger.info('Starting import')
    start_time = time.time()
    parser = argparse.ArgumentParser(description='Read Alma Analytics Report with Item Status and Update the Item Status in the database')
    parser.add_argument('-f', '--file', required=True, help='Input Alma Analytics Tab separated File')
    args = parser.parse_args()
    total_cnt = import_file(args.file)
    total_time = time.time() - start_time
    logger.info(f'Processed {total_cnt} in {total_time}')
    logger.info(f'Records/Second: {total_cnt / total_time}')

if __name__ == '__main__':
    main()