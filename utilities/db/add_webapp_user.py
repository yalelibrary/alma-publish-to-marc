import boto3
import psycopg2
from psycopg2 import sql
import argparse
import json
import secrets
import string


def generate_password(length):
    stringSource = string.ascii_letters + string.digits + '$%^!@'
    password = ''
    for i in range(length):
        password += secrets.choice(stringSource)
    return password


def add_webapp_user(config):
    password_for_webapp = generate_password(25)
    username_for_webapp = f'{config['app']['database']}_webapp'

    print(f'Host: {config['app']['host']}')
    print(f'Database: {config['app']['database']}')
    print(f'Webapp Username: {username_for_webapp}')
    print(f'Webapp Password: {password_for_webapp}')

    info = {
            'username': username_for_webapp,
            'password': password_for_webapp,
            'engine': 'postgres',
            'host': config['app']['host'],
            'port': '5432',
            'dbname': config['app']['database'],
            'dbInstanceIdentifier': config['app']['host'].split('.')[0]
        }
    # create user as root
    connection = psycopg2.connect(
        user=config['root']['username'],
        password=config['root']['password'],
        host=config['app']['host'],
        port="5432",
        database=config['app']['database'],
    )
    connection.set_session(autocommit=True)
    try:
        with connection.cursor() as c:
            c.execute(sql.SQL('create user {username} with encrypted password %(password)s').format(username=sql.Identifier(username_for_webapp)), {'password': password_for_webapp})
            c.execute(sql.SQL('GRANT CONNECT ON DATABASE {database} to {username}').format(database=sql.Identifier(config['app']['database']), username=sql.Identifier(username_for_webapp)))
    except:
        with connection.cursor() as c:
            c.execute(sql.SQL('ALTER USER {username} WITH PASSWORD %(password)s').format(username=sql.Identifier(username_for_webapp)), {'password': password_for_webapp})

    connection.close()

    # give permissions as app user
    connection = psycopg2.connect(
        user=config['app']['username'],
        password=config['app']['password'],
        host=config['app']['host'],
        port="5432",
        database=config['app']['database'],
    )
    connection.set_session(autocommit=True)
    with connection.cursor() as c:
        c.execute(sql.SQL('GRANT USAGE ON SCHEMA public to {username}').format(username=sql.Identifier(username_for_webapp)))
        c.execute(sql.SQL('GRANT ALL ON TABLE public.user_details TO {username}').format(username=sql.Identifier(username_for_webapp)))
        c.execute(sql.SQL('GRANT USAGE, SELECT ON SEQUENCE public.user_details_id_seq TO {username}').format(username=sql.Identifier(username_for_webapp)))
    connection.close()
    return info

def lookup_secrets(id):
    client = boto3.client('secretsmanager')
    secret = client.get_secret_value(SecretId=id)
    return json.loads(secret['SecretString'])

def store_secrets(name, value):
    client = boto3.client('secretsmanager')
    try:
        client.create_secret(
            ClientRequestToken=secrets.token_urlsafe(),
            Name=name,
            SecretString=json.dumps(value),
        )
    except:
        client.put_secret_value(
            ClientRequestToken=secrets.token_urlsafe(),
            SecretId=name,
            SecretString=json.dumps(value),
        )

def main():
    parser = argparse.ArgumentParser(description='Add webapp user')
    parser.add_argument('--secret-id', required=True, help='Secret store id')
    parser.add_argument('--app-secret-id', required=False, help='App secret store id')
    parser.add_argument('--save-secret-id', required=False, help='Secret store id for created users')
    args = parser.parse_args()
    app_secret = args.app_secret_id if args.app_secret_id else f'{args.secret_id}/app'
    save_secret_id = args.save_secret_id if args.save_secret_id else f'{app_secret.replace('/app', '')}/webapp'
    print(f'Saving in {save_secret_id}')

    config = {'app': lookup_secrets(app_secret), 'root': lookup_secrets(args.secret_id)}
    info = add_webapp_user(config)
    store_secrets(save_secret_id, info)

if __name__ == '__main__':
    main()