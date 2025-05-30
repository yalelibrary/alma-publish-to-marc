#
#  Read IDs from marc files
#
import os
from pymarc import XmlHandler, parse_xml
import tarfile
import logging
import argparse

logger = logging.getLogger(__name__)

class PymarcXmlHandler(XmlHandler):
    def __init__(self):
        self.cnt = 0
        self._record = None
        self._field = None
        self._subfield_code = None
        self._text = []
        self._strict = False
        self.normalize_form = None

    def process_record(self, record):
        print(record['001'].data)
        self.cnt += 1

def parse_file(file):
    with open(file, 'rb') as fh:
        tar = None
        fh1 = fh
        if file.endswith('gz'):
            tar = tarfile.open(fileobj=fh, mode="r:gz")
            for member in tar.getmembers():
                f = tar.extractfile(member)
                if f is not None:
                    fh1 = f
        if file.endswith('xml') or file.endswith('gz'):
            handler = PymarcXmlHandler()
            parse_xml(fh1, handler)

def main():
    parser = argparse.ArgumentParser(description='Read IDs from MARC and print')
    parser.add_argument('-f', '--files', nargs='?', help='File or directory of marc records')

    options = parser.parse_args()
    publish_files = options.files

    if os.path.isfile(publish_files):
        parse_file(publish_files)
    elif os.path.isdir(publish_files):
        for file in os.listdir(publish_files):
            if "delete" in file:
                continue
            parse_file(os.path.join(publish_files, file))

if __name__ == '__main__':
    main()

