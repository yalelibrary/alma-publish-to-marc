import argparse
from pymarc import MARCWriter
import json
from alma_publish_parser import process_publish_marc
import logging

logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description='Read Alma Publish MARC and output bib and holding MARC and Item JSON')
    parser.add_argument('-p', '--publish-file', nargs='?', help='Input Alma publish file or directory')
    parser.add_argument('-b', '--bib-file', nargs='?', help='Output bib MARC file')
    parser.add_argument('-m', '--holding-file', nargs='?', help='Output holding MARC file')
    parser.add_argument('-i', '--item-file', nargs='?', help='Output item JSONL file')
    options = parser.parse_args()
    with open(options.bib_file, 'wb') as bib_file, open(options.holding_file, 'wb') as holding_file, open(options.item_file, 'w') as item_file:
        bib_marc_writer = MARCWriter(bib_file)
        holding_marc_writer = MARCWriter(holding_file)
        def message(*msg):
            logger.info(*msg)
        def process_marc(mms_id, record, holding_ids):
            bib_marc_writer.write(record)
        def process_holding(mms_id, holding_id, holding_record):
            holding_marc_writer.write(holding_record)
        def process_item(item_json):
            item_file.write(json.dumps(item_json) + '\n')
        callback = {
            'message': message,
            'process_marc': process_marc,
            'process_holding': process_holding,
            'process_item': process_item
        }
        def create_callback():
            return callback
        process_publish_marc(options.publish_file, 1, create_callback) # max_workers = 1 so concurrent writing to file doesn't cause issues
        bib_marc_writer.close()
        holding_marc_writer.close()

if __name__ == '__main__':
    main()