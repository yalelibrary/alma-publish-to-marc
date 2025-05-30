from database_insert import StatementExecutor, generate_insert_prepared_statements, generate_insert_link_prepared_statement
import alma_client
import logging


logger = logging.getLogger(__name__)

library_insert = ''
location_insert = ''
circ_desk_insert = ''
location_circ_desk_insert = ''
code_table_insert = ''

def prepare_statements(database):
    global library_insert, location_insert, circ_desk_insert, location_circ_desk_insert, code_table_insert
    sql = generate_insert_prepared_statements('library_insert', 'library',
                                        ['code',
                                         'name',
                                         'alma_id',
                                         'path',
                                         'description',
                                         'campus',
                                         'campus_description'])
    database.execute_statement(sql['prepared_statement'])
    library_insert = sql['execute_statement']

    sql = generate_insert_prepared_statements('location_insert', 'location',
                                        ['code',
                                         'library_code',
                                         'external_name',
                                         'name',
                                         'suppress'], 2)
    database.execute_statement(sql['prepared_statement'])
    location_insert = sql['execute_statement']

    sql = generate_insert_prepared_statements('circ_desk_insert', 'circ_desk',
                                        ['code',
                                         'library_code',
                                         'name',
                                         'primary_desk',
                                         'reading_room_desk'], 2)
    database.execute_statement(sql['prepared_statement'])
    circ_desk_insert = sql['execute_statement']

    sql = generate_insert_link_prepared_statement('location_circ_desk_insert', 'location_circ_desk', ['circ_desk_id', 'location_id'])
    database.execute_statement(sql['prepared_statement'])
    location_circ_desk_insert = sql['execute_statement']

    sql = generate_insert_prepared_statements('code_table_insert', 'code_table_value', ['code_table', 'code', 'description'], 2 )
    database.execute_statement(sql['prepared_statement'])
    code_table_insert = sql['execute_statement']


location_id_dict = {}

def load_locations_from_db():
    with StatementExecutor() as database:
        sql = 'select id, library_code, code from location'
        for result in database.execute_query(sql):
            location_id_dict[(result[1], result[2])] = result[0]

def lookup_location(location):
    return location_id_dict.get(location, None)

def store_libraries(database):
    library_codes = []
    libraries = alma_client.load_libraries()
    for library in libraries['library']:
        params = {'code': library['code'],
                  'name': library['name'],
                  'alma_id': library['id'],
                  'path': library['path'],
                  'description': library.get('description', None),
                  'campus': library['campus'].get('value', None),
                  'campus_description': library['campus'].get('description', None)
                  }
        library_codes.append(library['code'])
        database.execute_statement(library_insert, params)
    return library_codes

def store_locations(database, library_codes):
    cnt = 0
    for library_code in library_codes:
        locations = alma_client.load_locations(library_code)['location']
        for location in locations:
            params = {'code': location['code'],
                    'library_code': library_code,
                    'external_name': location['external_name'],
                    'name': location['name'],
                    'suppress': 'N' if location['suppress_from_publishing'] == 'false' else 'Y'
                    }
            database.execute_statement(location_insert, params)
            cnt += 1
    return cnt

def store_circ_desks(database, library_codes):
    cnt = 0
    for library_code in library_codes:
        circ_desks = alma_client.load_circ_desks(library_code)['circ_desk']
        for circ_desk in circ_desks:
            code = circ_desk['code']
            name = circ_desk['name']
            primary = circ_desk['primary']
            reading_room_desk = circ_desk['reading_room_desk']
            location_ids = []
            for location in circ_desk['location']:
                location_id = lookup_location((library_code, location['location_code']))
                if location_id:
                    location_ids.append(location_id)
            params = {
                'code': code,
                'name': name,
                'primary_desk': primary,
                'reading_room_desk': reading_room_desk,
                'library_code': library_code
            }
            database.execute_statement(circ_desk_insert, params)
            cnt += 1
            database.commit()
            for row in database.execute_query('select id from circ_desk where name = %(name)s and library_code = %(library_code)s', params):
                circ_desk_id = row[0]
            for location_id in location_ids:
                params = {
                    'circ_desk_id': circ_desk_id,
                    'location_id': location_id
                }
                database.execute_statement(location_circ_desk_insert, params)
    return cnt

def store_code_tables(database):
    cnt = 0
    code_tables = ['BaseStatus', 'ItemPolicy']
    for code_table in code_tables:
        code_table_values = alma_client.load_code_table(code_table)['row']
        for code_table_value in code_table_values:
            params = {'code_table':code_table,
                    'code': code_table_value['code'],
                    'description': code_table_value['description']}
            cnt += 1
            database.execute_statement(code_table_insert, params)
    return cnt

def create_or_update_config_from_alma():
    with StatementExecutor() as database:
        prepare_statements(database)
        library_codes = store_libraries(database)
        logger.info(f'processed {len(library_codes)} libraries')
        cnt = store_locations(database, library_codes)
        logger.info(f'processed {cnt} locations')
        database.commit()
        load_locations_from_db()
        cnt = store_circ_desks(database, library_codes)
        logger.info(f'processed {cnt} circ_desks')
        cnt = store_code_tables(database)
        logger.info(f'processed {cnt} code table values')

if __name__ == '__main__':
    create_or_update_config_from_alma()