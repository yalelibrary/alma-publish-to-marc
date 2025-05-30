#
# Downloaed files from SFTP server
#
# Help:
#    python3 sftp_download.py -h
#
# Example usage:
#    python3 sftp_download.py -p alma-data/lit-publish -k ~/.ssh/alma_prod_rsa -u alma_prod -d /SML/Catalog/Solr/Alma/Published_Files/{datetime}-full-publish
#    python3 sftp_download.py -p alma-data/lit-publish -k ~/.ssh/alma_prod_rsa -u alma_prod -d /SML/Catalog/Solr/Alma/Published_Files/{datetime}-full-publish --delete
#    python3 sftp_download.py -p alma-data/lit-publish -k ~/.ssh/alma_prod_rsa -u alma_prod -d /SML/Catalog/Solr/Alma/Published_Files/{datetime}-full-publish --delete --wait-for-stable
#


from pathlib import Path
from paramiko.client import SSHClient
from sftp_wait_for_stable import wait_for_stable
import os
import argparse
import time
import logging


logger = logging.getLogger(__name__)

def file_progress(progress, total):
    logger.debug(f'{progress} / {total}')

def download_files(host, key, username, subdirectory, destination, host_keys_file, delete_on_download):
    destination_directory = False
    destination = destination.replace('{datetime}', time.strftime("%Y%m%d-%H%M%S"))
    client = SSHClient()
    client.load_system_host_keys()
    if host_keys_file:
        client.load_host_keys(host_keys_file)
    client.connect(host, username=username, key_filename=key)
    sftp_client = client.open_sftp()
    sftp_client.chdir(subdirectory)
    cnt = 0
    for file in sftp_client.listdir():
        sftp_size = sftp_client.stat(f'{file}').st_size
        logger.info(f'downloading {file} with size {sftp_size}')
        if not os.path.exists(destination):
            logger.info(f'Downloading file to: {destination}')
            os.makedirs(destination)
        destination_directory = destination
        destination_file = os.path.join(destination, file)
        sftp_client.get(file, destination_file, callback=file_progress)
        local_size = os.stat(destination_file).st_size
        logger.info(f'{local_size} / {sftp_size} : complete')
        cnt += 1
        if local_size == sftp_size:
            if delete_on_download:
                sftp_client.remove(file)
                logger.info(f'deleted ftp://{os.path.join(host, subdirectory, file)}')
        else:
            logger.info(f'error downloading {file}')
    if not cnt:
        logger.info(f'No files where downloaded')
    else:
        logger.info(f'{cnt} files downloaded to: {destination}')
    return destination_directory

def main():
    host_keys_file = Path(__file__).with_name('sftp_host_keys')
    parser = argparse.ArgumentParser(description='Download files from SFTP server')
    parser.add_argument('-s', '--server', nargs='?', help='SFTP server')
    parser.add_argument('-k', '--key', help='Key filename')
    parser.add_argument('-p', '--path', help='Path to files', default = 'alma_data/lit-publish')
    parser.add_argument('-u', '--user', help='User', default = 'alma_prod')
    parser.add_argument('-d', '--destination', help='Destination directory', default = './')
    parser.add_argument('--host-keys-file', help='Host keys file', default=host_keys_file)
    parser.add_argument('--wait-for-stable', action='store_true', help='Wait for files to be stable')
    parser.add_argument('--delete', action='store_true', help='Delete remote file after download')
    options = parser.parse_args()
    if options.wait_for_stable:
        if not wait_for_stable(options.server, options.key, options.user, options.path, options.host_keys_file):
            logger.info("No files found")
            return

    download_files(options.server, options.key, options.user, options.path, options.destination, options.host_keys_file, options.delete)

if __name__ == '__main__':
    main()
