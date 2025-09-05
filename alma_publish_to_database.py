#
#  Store marc and item json in database
#
import os
import argparse
import marc.helper
from marc.helper import to_marc_xml
import json
from pathlib import Path
from database_insert import StatementExecutor, generate_insert_prepared_statements, generate_insert_link_prepared_statement, configure_inserts
from alma_publish_parser import process_publish_marc
from alma_config_to_database import create_or_update_config_from_alma
import time
import logging
from sftp_download import wait_for_stable, download_files
from psycopg2 import sql
import re
import metrics.aws_metrics

logger = logging.getLogger(__name__)

bib_field = os.getenv('BIB_FIELD_TAG') or 'BIB'
holding_field = os.getenv('HOLDING_FIELD_TAG') or 'HLD'

institution_id = '8651'
holding_prefix = '22'
bib_prefix = '99'
location_id_dict = {}
bib_brief_insert = None
holding_brief_insert = None
holding_marc_insert = None
item_insert = None
item_data_insert = None
item_base_status_insert = None
bib_part_insert = None
bib_marc_xml_insert = None
item_delete = None

def configure_logging(name):
    root_logger = logging.getLogger()
    log_level = os.getenv("LOG_LEVEL", "INFO")
    root_logger.setLevel(log_level)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    fh_info = logging.FileHandler(name)

    fh_info.setFormatter(formatter)
    fh_info.setLevel(log_level)

    root_logger.addHandler(fh_info)


def prepare_statements(database):
    global bib_brief_insert, bib_marc_insert, holding_brief_insert, holding_marc_insert, item_insert, item_data_insert, item_base_status_insert, bib_part_insert, item_delete, bib_marc_xml_insert
    sql = generate_insert_prepared_statements('bib_brief_insert', 'bib_brief',
                          ['mms_id',
                           'title',
                           'author',
                           'system_create_date_time',
                           'system_update_date_time',
                           'voyager_bib_id',
                           'publication_date',
                           'publication_place',
                           'publisher',
                           'field008',
                           'extent',
                           'material',
                           'leader',
                           'issn',
                           'isbn',
                           'oclc_number',
                           'suppress'])
    bib_brief_insert = sql['execute_statement']
    database.execute_statement(sql['prepared_statement'])
    sql = generate_insert_prepared_statements('bib_marc_insert', 'bib_marc', ['mms_id', 'raw_marc'])
    database.execute_statement(sql['prepared_statement'])
    bib_marc_insert = sql['execute_statement']
    sql = generate_insert_prepared_statements('bib_marc_xml_insert', 'bib_marc_xml', ['mms_id', 'marc_xml'])
    bib_marc_xml_insert = sql['execute_statement']
    database.execute_statement(sql['prepared_statement'])
    sql = generate_insert_prepared_statements('holding_brief_insert', 'holding_brief',
                          ['holding_id',
                           'mms_id',
                           'display_call_number',
                           'location_id',
                           'system_create_date_time',
                           'system_update_date_time',
                           'field008',
                           'voyager_holding_id',
                           'suppress' ])
    holding_brief_insert = sql['execute_statement']
    database.execute_statement(sql['prepared_statement'])
    sql = generate_insert_prepared_statements('holding_marc_insert', 'holding_marc', ['holding_id', 'raw_marc'])
    holding_marc_insert = sql['execute_statement']
    database.execute_statement(sql['prepared_statement'])
    sql = generate_insert_prepared_statements('item_insert', 'item',
                          ['pid',
                           'holding_id',
                           'perm_location_id',
                           'temp_location_id',
                           'sequence_number',
                           'item_enum',
                           'chron',
                           'barcode',
                           'system_create_date_time',
                           'system_update_date_time',
                           'material_type',
                           'pieces',
                           'copy_id',
                           'policy',
                           'description',
                           'inventory_date_time',
                           'voyager_item_id'])
    item_insert = sql['execute_statement']
    database.execute_statement(sql['prepared_statement'])
    sql = generate_insert_prepared_statements('item_data_insert', 'item_data', ['pid', 'data'])
    item_data_insert = sql['execute_statement']
    database.execute_statement(sql['prepared_statement'])
    sql = generate_insert_prepared_statements('item_base_status_insert', 'item_base_status', ['pid', 'status_code', 'process_type'], 1, 'where item_base_status.update_date_time < $4', ['system_update_date'])
    item_base_status_insert = sql['execute_statement']
    database.execute_statement(sql['prepared_statement'])
    bib_part_insert = 'insert into bib_part (mms_id, part_mms_id) values (%(mms_id)s, %(part_mms_id)s) on conflict do nothing'
    item_delete = 'delete from item where pid = %(pid)s'

def load_locations():
    with StatementExecutor() as database:
        sql = 'select id, library_code, code from location'
        for result in database.execute_query(sql):
            location_id_dict[(result[1], result[2])] = result[0]

def lookup_location(location):
    return location_id_dict.get(location, None)

def store_bib_marc(database, mms_id, record, holding_ids):
    pid_sql = 'select holding_id from holding_brief where mms_id = %(mms_id)s and holding_id != ALL(%(holding_ids)s)'
    for result in database.execute_query(pid_sql, {'mms_id': mms_id, 'holding_ids': holding_ids}):
        holding_to_delete = result[0]
        delete_holding_record(database, holding_to_delete)
    dates = marc.helper.extract_system_dates(record, bib_field)
    suppress = 'Y' if marc.helper.subfields_as_string(record, bib_field, 'a') != 'false' else 'N'
    issns = marc.helper.extract_issns(record)
    oclcs = marc.helper.extract_oclcs(record)
    isbns = marc.helper.extract_isbns(record)

    params = {'mms_id': mms_id,
              'title': marc.helper.extract_title(record),
              'author': marc.helper.extract_author(record),
              'publication_date': marc.helper.extract_publication_date(record),
              'publication_place': marc.helper.extract_publication_place(record),
              'publisher': marc.helper.extract_publisher(record),
              'system_create_date_time': dates[0],
              'system_update_date_time': dates[1],
              'field008': marc.helper.extract_fixed_field(record, '008'),
              'extent': marc.helper.extract_extent(record),
              'material': marc.helper.extract_material(record),
              'leader': record.leader,
              'issn': issns[0] if issns else None,
              'oclc_number': oclcs[0] if oclcs else None,
              'isbn': isbns[0] if isbns else None,
              'voyager_bib_id': marc.helper.extract_voyager_or_sierra_id(record),
              'suppress': suppress}

    try:
        database.execute_statement(bib_brief_insert, params)
        params = {'mms_id': mms_id, 'marc_xml': to_marc_xml(record)}
        database.execute_statement(bib_marc_xml_insert, params)

        # delete AVE and AVD fields, leave BIB field
        record.remove_fields('AVE', 'AVD')
        params = {'mms_id': mms_id, 'raw_marc': record.as_marc()}
        if len(params['raw_marc']) > 99999:
            record.remove_fields('BIB') # try removing the BIB field in case that brings it over the max
            params['raw_marc'] == record.as_marc()
            if len(params['raw_marc']) > 99999:
                logger.error(f'\nSkipping record because it exceeds the maximum size for binary MARC records: {mms_id}\n')
                return
            else:
                logger.warning(f'\nRemoved BIB field from bib record because it exceeded the maximum size for binary MARC records: {mms_id}\n')
        database.execute_statement(bib_marc_insert, params)
        store_constituent_units(database, mms_id, record)
        if oclcs:
            oclcs = [x for x in [format_oclc(o) for o in oclcs] if x]
            insert_external_ids(database, mms_id, 'bib', 'oclc', oclcs)
        if issns:
            issns = [x for x in [format_number_dashes(i) for i in issns] if x]
            insert_external_ids(database, mms_id, 'bib', 'issn', issns)
        if isbns:
            isbns = [x for x in [format_number_dashes(i) for i in isbns] if x]
            insert_external_ids(database, mms_id, 'bib', 'isbn', isbns)
        database.commit()
    except:
        database.rollback()
        raise

def format_oclc(o):
    if not o:
        return None
    o = re.sub(r'\D', '', o)
    if len(o) <= 8:
        return f'ocm{o}'
    if len(o) == 9:
        return f'ocn{o}'
    return f'on{o}'

def format_number_dashes(n):
    if not n:
        return None
    return re.sub(r'[^0-9-]', '', n)


def store_constituent_units(database, mms_id, record):
    database.execute_statement('delete from bib_part where mms_id = %(mms_id)s', {'mms_id': mms_id})
    for field in record.get_fields('774'):
        subfieldw = field.get_subfields('w')
        if subfieldw and subfieldw[0].startswith(bib_prefix) and subfieldw[0].endswith(institution_id):
            database.execute_statement(bib_part_insert, {'mms_id': mms_id, 'part_mms_id': subfieldw[0]})

def insert_external_ids(database, record_id, record_type, external_id_type, external_ids):
    database.execute_statement('delete from external_id where record_id = %(record_id)s and external_id_type = %(external_id_type)s', {'record_id': record_id, 'external_id_type': external_id_type})
    if external_ids:
        for external_value in set(external_ids):
            database.execute_statement('insert into external_id (create_date_time, record_id, record_type, external_id_type, external_value) values (NOW(), %(record_id)s, %(record_type)s, %(external_id_type)s, %(external_value)s)',
                                       {
                                        'record_id': record_id,
                                        'record_type': record_type,
                                        'external_id_type': external_id_type,
                                        'external_value': external_value
                                       })


def store_holding_marc(database, mms_id, holding_id, holding_marc, pids):
    # insert or update holding_brief
    pid_sql = 'select pid from item where holding_id = %(holding_id)s and pid != ALL(%(pids)s)'
    for result in database.execute_query(pid_sql, {'holding_id': holding_id, 'pids': pids}):
        pid_to_delete = result[0]
        database.execute_statement(item_delete, {'pid': pid_to_delete})
    dates = marc.helper.extract_system_dates(holding_marc, holding_field)
    suppress = 'Y' if marc.helper.subfields_as_string(holding_marc, holding_field, 'a') != 'false' else 'N'
    params = {'holding_id': holding_id,
              'mms_id': mms_id,
              'display_call_number': marc.helper.extract_call_number(holding_marc),
              'location_id': lookup_location(marc.helper.extract_library_and_location_code(holding_marc)),
              'system_create_date_time': dates[0],
              'system_update_date_time': dates[1],
              'field008': marc.helper.extract_fixed_field(holding_marc, '008'),
              'voyager_holding_id': marc.helper.extract_voyager_or_sierra_id(holding_marc),
              'suppress': suppress}
    try:
        database.execute_statement(holding_brief_insert, params)
        params = {'holding_id': holding_id, 'raw_marc': holding_marc.as_marc()}
        if len(params['raw_marc']) > 99999:
            holding_marc.remove_fields(holding_field) # try removing the HLD field in case that brings it over the max (see: 2292646080008651)
            params['raw_marc'] = holding_marc.as_marc()
            if len(params['raw_marc']) > 99999:
                logger.error(f'\nSkipping holding record because it exceeds the maximum size for binary MARC records: {holding_id}\n')
                return
            else:
                logger.warning(f'\nRemoved HLD field from holding record because it exceeded the maximum size for binary MARC records: {holding_id}\n')
        database.execute_statement(holding_marc_insert, params)
        database.commit()
    except:
        database.rollback()
        raise

def rm_ws(str):
    if str:
        return re.sub(' +', ' ', str)

def presence(str):
    if str:
        return str

def store_item_json(database, json_string):
    json_obj = json.loads(json_string)
    in_temp = json_obj['holding_data'].get('in_temp_location', False)
    pid = json_obj['item_data']['pid']
    holding_id = json_obj['holding_data']['holding_id']
    perm_library_code = json_obj['item_data'].get('library', {}).get('value', None)
    perm_location_code = json_obj['item_data'].get('location', {}).get('value', None)
    temp_library_code = json_obj['holding_data'].get('temp_library', {}).get('value', None) if in_temp else None
    temp_location_code = json_obj['holding_data'].get('temp_location', {}).get('value', None) if in_temp else None
    status_code = json_obj['item_data'].get('base_status', {}).get('value', '1')
    description = rm_ws(presence(json_obj['item_data'].get('description', '')))
    perm_location_id = lookup_location((perm_library_code, perm_location_code))
    temp_location_id = lookup_location((temp_library_code, temp_location_code))
    enumeration_a = presence(json_obj['item_data'].get('enumeration_a', ''))
    chronology_i = presence(json_obj['item_data'].get('chronology_i', ''))
    barcode = presence(json_obj['item_data'].get('barcode', ''))
    creation_date = marc.helper.parse_date_str(json_obj['item_data'].get('creation_date', ''))
    modification_date = marc.helper.parse_date_str(json_obj['item_data'].get('modification_date', ''))
    inventory_date_time = marc.helper.parse_date_str(json_obj['item_data'].get('inventory_date', ''))
    material_type = presence(json_obj['item_data'].get('physical_material_type', {}).get('value', ''))
    pieces = presence(json_obj['item_data'].get('pieces',''))
    inventory_number = presence(json_obj['item_data'].get('inventory_number',''))
    copy_id = presence(json_obj['holding_data'].get('copy_id',''))
    item_policy = presence(json_obj['item_data'].get('policy', {}).get('value', ''))
    process_type = presence(json_obj['item_data'].get('process_type', {}).get('value', ''))
    params = {'pid': pid,
              'holding_id': holding_id,
              'perm_location_id': perm_location_id,
              'temp_location_id': temp_location_id,
              'sequence_number': 1,
              'chron': chronology_i,
              'item_enum': enumeration_a,
              'barcode': barcode,
              'system_create_date_time': creation_date,
              'system_update_date_time': modification_date,
              'inventory_date_time': inventory_date_time,
              'material_type': material_type,
              'description': description,
              'pieces': pieces,
              'copy_id': copy_id,
              'policy': item_policy,
              'voyager_item_id': inventory_number}
    try:
        database.execute_statement(item_insert, params)
        params = {'pid': pid, 'data': json_string}
        database.execute_statement(item_data_insert, params)
        params = {'pid': pid, 'status_code': status_code, 'system_update_date': modification_date, 'process_type': process_type}
        database.execute_statement(item_base_status_insert, params)
        database.commit()
    except:
        database.rollback()
        raise

def delete_bib_record(database, mms_id):
    bib_delete_sql = 'DELETE from bib_brief where mms_id = %(mms_id)s'
    database.execute_statement(bib_delete_sql, {'mms_id': mms_id})

def delete_holding_record(database, holding_id):
    holding_delete_sql = 'DELETE from holding_brief where holding_id = %(holding_id)s'
    database.execute_statement(holding_delete_sql, {'holding_id': holding_id})

def initialize_database(full):
    p = Path(__file__).with_name('ddl')
    p = Path(os.path.join(p, 'data_sync_db.sql'))
    with p.open('r') as f, StatementExecutor() as se:
        statements = ' '.join(f.readlines()).split(';')
        username_for_webhook = f'{os.getenv("DB_SCHEMA")}_webhook'
        username_for_webapp = f'{os.getenv("DB_SCHEMA")}_webapp'
        username_for_ro = f'{os.getenv("DB_SCHEMA")}_ro'
        for statement in statements:
            statement = statement.strip()
            if full and statement.startswith('# FULL'):
                statement = statement.replace('# FULL', '')
            if statement:
                if statement.startswith('# FULL'):
                    continue
                try:
                    se.execute_statement(statement.replace('|', ';'))
                    se.commit()
                except Exception as e:
                    logger.exception(f'Error initializing the database: {e}')
                    se.rollback()
        statements = [
            sql.SQL('GRANT USAGE ON SCHEMA public to {username}').format(username=sql.Identifier(username_for_webhook)),
            sql.SQL('GRANT ALL ON TABLE public.ITEM_BASE_STATUS TO {username}').format(username=sql.Identifier(username_for_webhook)),
            sql.SQL('GRANT ALL ON TABLE public.record_update TO {username}').format(username=sql.Identifier(username_for_webhook)),
            sql.SQL('GRANT USAGE, SELECT ON SEQUENCE public.record_update_id_seq TO {username}').format(username=sql.Identifier(username_for_webhook)),
            sql.SQL('GRANT ALL ON TABLE public.request_event TO {username}').format(username=sql.Identifier(username_for_webhook)),
            sql.SQL('GRANT USAGE ON SCHEMA public to {username}').format(username=sql.Identifier(username_for_webapp)),
            sql.SQL('GRANT ALL ON TABLE public.user_details TO {username}').format(username=sql.Identifier(username_for_webapp)),
            sql.SQL('GRANT USAGE, SELECT ON SEQUENCE public.user_details_id_seq TO {username}').format(username=sql.Identifier(username_for_webapp)),
            sql.SQL('GRANT SELECT ON ALL TABLES IN SCHEMA public TO {username}').format(username=sql.Identifier(username_for_webapp))
        ]
        for result in se.execute_query("select usename from pg_catalog.pg_user where usename like '%_ro%'"):
            logger.info(f'Setting up permissions for readonly user {result[0]}')
            statements.append(sql.SQL('GRANT USAGE ON SCHEMA public to {username}').format(username=sql.Identifier(result[0])))
            statements.append(sql.SQL('GRANT SELECT ON ALL TABLES IN SCHEMA public TO {username}').format(username=sql.Identifier(result[0])))

        for statement in statements:
            if statement:
                try:
                    se.execute_statement(statement)
                    se.commit()
                except Exception as e:
                    logger.exception(f'Error initializing the database: {e}')
                    se.rollback()



def download_from_sftp_server(options):
    if options.wait_for_stable:
        if not wait_for_stable(options.server, options.key, options.user, options.path, options.host_keys_file):
            logger.info("No files found")
            return False
    return download_files(options.server, options.key, options.user, options.path, options.destination, options.host_keys_file, options.delete)

def main():
    configure_logging(f'{Path(__file__).stem}.log')
    logger.info('Starting import')
    start_time = time.time()
    host_keys_file = Path(__file__).with_name('sftp_host_keys')

    parser = argparse.ArgumentParser(description='Read Alma Publish MARC and output bib and holding MARC and Item JSON')
    parser.add_argument('-p', '--publish-file', nargs='?', help='Input Alma publish file or directory')
    parser.add_argument('-n', '--max-workers', type=int, help='Maximum number of workers', default = 40)
    parser.add_argument('-c', '--process-config', action='store_true', help='Import configuration data using Alma API')
    parser.add_argument('--initialize-database', action='store_true', help='Initialize the database')
    parser.add_argument('--full-database-create', action='store_true', help='Drop and Create all tables during initialization')
    parser.add_argument('--no-updates', action='store_true', help='Do not perform updates for existing records')

    parser.add_argument('-f', '--sftp', action='store_true', help='Download from SFTP server')
    parser.add_argument('-s', '--server', nargs='?', help='SFTP server')
    parser.add_argument('-k', '--key', help='Key filename')
    parser.add_argument('--path', help='Path to files', default = 'alma-data/lit-publish')
    parser.add_argument('-u', '--user', help='User', default = 'alma_prod')
    parser.add_argument('-d', '--destination', help='Destination directory', default = './')
    parser.add_argument('--host-keys-file', help='Host keys file', default=host_keys_file)
    parser.add_argument('--wait-for-stable', action='store_true', help='Wait for files to be stable')
    parser.add_argument('--delete', action='store_true', help='Delete remote file after download')


    options = parser.parse_args()

    if options.sftp:
        options.publish_file = download_from_sftp_server(options)

    if not options.publish_file:
        logger.info("No published files")
        metrics.aws_metrics.send_metric('datasync', [
            {'name':'runs', 'value': 1},
            {'name':'files', 'value': 0},
            {'name':'ingest', 'value': 0},
            {'name':'error', 'value': 0},
            {'name':'delete', 'value': 0}
        ], 'database', os.getenv("DB_SCHEMA"))
        return

    configure_inserts(options.no_updates)

    if options.initialize_database:
        if input(f'Are you sure you want to initialize the database ({os.getenv("DB_SCHEMA")})? (Y/n)') != 'Y':
            exit()
        logger.info('\nInitializing database')
        initialize_database(options.full_database_create)

    if options.process_config:
        logger.info('\nImporting configuration')
        create_or_update_config_from_alma()

    logger.info('\nParsing files')
    load_locations()
    databases = []
    def create_callback():
        database = StatementExecutor()
        database.open()
        databases.append(database)
        prepare_statements(database)
        def message(*msg):
            logger.info(*msg)
        def process_marc(mms_id, record, holding_ids):
            store_bib_marc(database, mms_id, record, holding_ids)
        def process_holding(mms_id, holding_id, holding_record, pids):
            store_holding_marc(database, mms_id, holding_id, holding_record, pids)
        def process_item(item_json):
            store_item_json(database, item_json)
        def delete_bib(mms_id):
            delete_bib_record(database, mms_id)
        def delete_holding(holding_id):
            delete_holding_record(database, holding_id)
        def file_complete():
            database.commit()
            database.close()
            databases.remove(database)
        callback = {
            'message': message,
            'process_marc': process_marc,
            'process_holding': process_holding,
            'process_item': process_item,
            'delete_bib': delete_bib,
            'delete_holding': delete_holding,
            'file_complete': file_complete
        }
        return callback

    results = process_publish_marc(options.publish_file, options.max_workers, create_callback)

    for database in databases:
        logger.info('closing left over db')
        database.commit()
        database.close()
    logger.info(f'\nTotal elapsed {(time.time() - start_time)} seconds')

    if results['files'] == 0:
        metrics.aws_metrics.send_metric('datasync', [
            {'name':'runs', 'value': 1},
            {'name':'files', 'value': 0},
            {'name':'ingest', 'value': 0},
            {'name':'error', 'value': 0},
            {'name':'delete', 'value': 0}
        ], 'database', os.getenv("DB_SCHEMA"))
    else:
        metrics.aws_metrics.send_metric('datasync', [
            {'name':'files', 'value': results['files']},
            {'name':'ingest', 'value': results['ingest']},
            {'name':'error', 'value': results['error']},
            {'name':'delete', 'value': results['delete']},
            {'name':'runs', 'value': 1},
        ], 'database', os.getenv("DB_SCHEMA"))

if __name__ == '__main__':
    main()