#
# Wait for files on SFTP server to remain stable for 2 minutes
# Help:
#    python3 sftp_wait_for_stable.py -h
#
# Example usage:
#    python3 sftp_wait_for_stable.py -p alma-data/lit-publish -k ~/.ssh/alma_prod_rsa -u alma_prod
#


from pathlib import Path
from paramiko.client import SSHClient
import os
import argparse
import time
import logging

logger = logging.getLogger(__name__)


def stat_files(sftp_client):
    file_info = {}
    for file in sftp_client.listdir():
        sftp_size = sftp_client.stat(f'{file}').st_size
        sftp_mtime = sftp_client.stat(f'{file}').st_mtime
        file_info[file] = [sftp_size, sftp_mtime]
    return file_info


def wait_for_stable(host, key, username, subdirectory, host_keys_file):
    logger.info('Waiting for files to be stable')
    client = SSHClient()
    client.load_system_host_keys()
    if host_keys_file:
        client.load_host_keys(host_keys_file)
    client.connect(host, username=username, key_filename=key)
    sftp_client = client.open_sftp()
    sftp_client.chdir(subdirectory)
    prior_file_info = None
    while True:
        file_info = stat_files(sftp_client)
        if prior_file_info and file_info == prior_file_info:
            break
        prior_file_info = file_info
        if not file_info:
            return False
        time.sleep(60)
    logger.info('Files were stable for 1 minutes')
    return True

def main():
    host_keys_file = Path(__file__).with_name('sftp_host_keys')
    parser = argparse.ArgumentParser(description='Wait for files to be stable on SFTP server')
    parser.add_argument('-s', '--server', nargs='?', help='SFTP server')
    parser.add_argument('-k', '--key', help='Key filename')
    parser.add_argument('-p', '--path', help='Path to files', default = 'alma_data/lit-publish')
    parser.add_argument('-u', '--user', help='User', default = 'alma_prod')
    parser.add_argument('--host-keys-file', help='Host keys file', default=host_keys_file)
    options = parser.parse_args()
    wait_for_stable(options.server, options.key, options.user, options.path, options.host_keys_file)

if __name__ == '__main__':
    main()
