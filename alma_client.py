import requests
import os

token = os.getenv("ALMA_API_TOKEN")
base_url = os.getenv('ALMA_API_BASE_URL')

def load_libraries():
    uri = f'{base_url}/conf/libraries/'
    return get(uri)

def load_locations(library_code):
    uri = f'{base_url}/conf/libraries/{library_code}/locations'
    return get(uri)


def load_circ_desks(library_code):
    uri = f'{base_url}/conf/libraries/{library_code}/circ-desks'
    return get(uri)

def load_code_table(code_table_name):
    uri = f'{base_url}/conf/code-tables/{code_table_name}'
    return get(uri)


def load_sets(offset = 0):
    uri = f'{base_url}/conf/sets?limit=100&offset={offset}&set_origin=UI'
    return get(uri)

def load_set(set_id):
    uri = f'{base_url}/conf/sets/{set_id}'
    return get(uri)

def get(uri):
    response = requests.get(uri, headers={'Authorization':f'apikey {token}', 'accept': 'application/json'} )
    return response.json()

