import psycopg2
import logging
import os
no_updates = False

def configure_inserts(perform_updates):
    global no_updates
    no_updates = perform_updates


def generate_insert_prepared_statements(name, table_name, fields, conflict_field_count = 1, where = '', where_parameters = None):
    if no_updates:
        return generate_insert_prepared_statements_no_updates(name, table_name, fields)

    field_count = len(fields)
    arguments = [*fields]
    if where_parameters:
        arguments.extend(where_parameters)
    return {'prepared_statement':
    f'''
    prepare {name} as insert into {table_name} ({','.join(fields)}, version, create_date_time, update_date_time) values ({','.join([f'${x+1}' for x in range(field_count)])}, 0, now(), now())
    on conflict({','.join(fields[:conflict_field_count])}) do update set {','.join([f'{fields[x+1]} = ${x+2}' for x in range(len(fields)-1)])}, version = {table_name}.version + 1, update_date_time = now()
    {where}
    '''
    ,
    'execute_statement':
    f'''
    execute {name}({','.join([f'%({field})s' for field in arguments])})
    '''
    }


def generate_insert_prepared_statements_no_updates(name, table_name, fields):
    field_count = len(fields)
    arguments = [*fields]
    return {'prepared_statement':
    f'''
    prepare {name} as insert into {table_name} ({','.join(fields)}, version, create_date_time, update_date_time) values ({','.join([f'${x+1}' for x in range(field_count)])}, 0, now(), now())
    on conflict do nothing
    '''
    ,
    'execute_statement':
    f'''
    execute {name}({','.join([f'%({field})s' for field in arguments])})
    '''
    }


def generate_insert_link_prepared_statement(name, table_name, fields):
    return {'prepared_statement':
    f'''
    prepare {name} as insert into {table_name} ({','.join(fields)}) values ({','.join([f'${x+1}' for x in range(len(fields))])})
    on conflict do nothing
    '''
    ,
    'execute_statement':
    f'''
    execute {name}({','.join([f'%({field})s' for field in fields])})
    '''
    }


class StatementExecutor:
    def __init__(self):
        self._logger = logging.getLogger(__name__)
        self._connection = None
        self._cursor = None
        self._statement_cursor = None

    def __enter__(self):
        return self.open()

    def open(self):
        try:
            self._connection = psycopg2.connect(
                user=os.getenv("DB_USERNAME"),
                password=os.getenv("DB_PASSWORD"),
                host=os.getenv("DB_HOST"),
                port="5432",
                database=os.getenv("DB_SCHEMA"),
            )
            self._statement_cursor = self._connection.cursor()
            self._query_cursor = self._connection.cursor()

        except Exception as e:
            self._logger.error(f"Error establishing database connection: {e}")
            raise
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.close()

    def close(self):
        self.commit()
        if self._statement_cursor:
            self._statement_cursor.close()
        if self._query_cursor:
            self._query_cursor.close()
        if self._connection:
            self._connection.close()

    def execute_statement(self, sql, parameters=None):
        if not self._connection:
            raise Exception("Database connection not established.")
        self._statement_cursor.execute(sql, parameters)

    def execute_query(self, sql, parameters=None):
        if not self._connection:
            raise Exception("Database connection not established.")
        self._query_cursor.execute(sql, parameters)
        while True:
            records = self._query_cursor.fetchmany(1000)
            if not records:
                break
            for row in records:
                yield row

    def commit(self):
        self._connection.commit()

    def rollback(self):
        self._connection.rollback()