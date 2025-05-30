import os
from pymarc import Record, Field, Subfield, XmlHandler, MARCReader, parse_xml, marcxml
import json
import re
import tarfile
from pathlib import Path
import concurrent.futures
import time
import traceback
import logging
import glob

logger = logging.getLogger(__name__)

item_field = os.getenv('ITEM_FIELD_TAG') or 'ITM'

subfield_str_removal = re.compile(r'<\$.*?>')
institution_id = ('8651', '0521', '0541', '1021', '0951', '0121') # Yale + some sandbox ids
holding_prefix = '22'
cnt = 0
cnt_files = 0
cnt_bibs = 0
cnt_errors = 0
cnt_holdings = 0
cnt_items = 0
cnt_deletes = 0
holding_ids = set()
bib_ids = set()

def load_item_template():
    p = Path(__file__).with_name('item-template.json')
    with p.open('r') as f:
        json_obj = json.load(f) # load and dump to get on one line
        return json.dumps(json_obj,separators=(',', ':'))

class PymarcXmlHandler(XmlHandler):
    def __init__(self, item_template, callback_data, record_processor, publish_file):
        self._record = None
        self._field = None
        self._subfield_code = None
        self._text = []
        self._strict = False
        self.normalize_form = None
        self.bib_ids = bib_ids
        self.holding_ids = holding_ids
        self.item_template = item_template
        self.callback_data = callback_data
        self.record_processor = record_processor
        self.publish_file = publish_file
        self.cnt = 0

    def process_record(self, record):
        try:
            self.cnt += 1
            self.record_processor(record, self.item_template,  self.bib_ids, self.holding_ids, self.callback_data, self.publish_file)
        except Exception as e:
            traceback.print_exc()
            exit(1)

def extract_control_field_groups(record):
    # groups of control fields in holding order, each group starts and ends with an 009
    control_field_groups = []
    in_holding = False
    current_fld_set = []
    all_holding_control_fields = []
    tag_005_cnt = 0
    exit_holding = False
    for field in record:
        if not field.tag.startswith('00'):
            break
        if field.tag == '009' and in_holding: # 009 is the last holding control field, so exit if in_holding
            exit_holding = True
        if field.tag == '005':
            tag_005_cnt += 1
            if tag_005_cnt > 1: # if it's anything but the bibs 005, force in_holding
                in_holding = True
        if field.tag == '009' or field.tag == '003' or field.tag == '002': # if it's a special field, force in_holding
            in_holding = True
        if in_holding: # at lease one 009 has been encountered
            current_fld_set.append(field)
            all_holding_control_fields.append(field)
        if exit_holding: # add group and reset on 2nd 009
            control_field_groups.append(current_fld_set)
            current_fld_set = []
            in_holding = False
            exit_holding = False
    for holding_control_field in all_holding_control_fields:
        record.remove_field(holding_control_field)
    return control_field_groups

def handle_record(record, item_template, bib_ids, holding_ids, callback, publish_file):
    global cnt, cnt_bibs, cnt_holdings, cnt_items, cnt_errors
    cnt += 1
    # fix 001 not first field in Alma MARC XML, so move it to first field
    fld001 = record.get_fields('001')[0]
    mms_id = fld001.value()
    try:
        # move 001 to first field
        record.fields.insert(0, record.fields.pop(record.fields.index(fld001)))

        holding_records_by_id = {}
        control_field_groups = extract_control_field_groups(record)

        # create the holding marc records in order using the 852, since they should all have that field
        flds852 = [fld for fld in record.get_fields('852') if get_holding_id_subfield(fld)]

        bibs_holding_ids = list(set([get_holding_id_subfield(fld).value for fld in flds852]))
        holding_count = len(bibs_holding_ids)
        if holding_count != len(control_field_groups):
            raise Exception(f'Holding Count does not match control field groups: {mms_id}')

        holding_index = 0
        for field in flds852:
            subfield = get_holding_id_subfield(field)
            if subfield and not subfield.value in holding_records_by_id:
                control_fields = control_field_groups[holding_index]
                leaders_and_ids = [c for c in control_fields if c.tag == '009']
                holding005s = [c for c in control_fields if c.tag == '005']
                holding007s = [c for c in control_fields if c.tag == '002']
                holding008s = [c for c in control_fields if c.tag == '003']
                holding_index += 1
                fld001 = None

                if len(leaders_and_ids) == 1:
                    leader = leaders_and_ids[0].data
                    fld001 = None
                elif len(leaders_and_ids) != 2:
                    raise Exception(f'Holding leader and id is not 2 for mmsid: {mms_id} and holding id {subfield.value}')
                else:
                    fld001 = leaders_and_ids[0].data
                    leader = leaders_and_ids[1].data
                    if leader.isdigit() and not fld001.isdigit():
                        raise Exception(f'Leader and fld001 in bib look problematic for mmsid: {mms_id} and holding id {subfield.value}')
                holding_record = Record(file_encoding='utf-8', leader=leader)
                holding_record.add_field(Field(tag='001', data=subfield.value))
                holding_record.add_field(Field(tag='004', data=mms_id))
                if fld001: # copy the Alma 001 to the original holding id field in 035, if 001 is not an Alma holding ID
                    if not(fld001.startswith(holding_prefix) and fld001.endswith(institution_id)):
                        if fld001.isdigit() and len(fld001) < 9:
                            holding_record.add_field(Field(tag='035', indicators=[' ', ' '], subfields=[Subfield(code='a', value=f'(CtY){fld001}-yaledb-Voyager')]))
                        if 'yale_inst' in fld001:
                            holding_record.add_field(Field(tag='035', indicators=[' ', ' '], subfields=[Subfield(code='a', value=f'(CtY){fld001.replace("yale_inst","")}-yaledb-Other')]))
                if (holding005s):
                    holding_record.add_field(holding005s[0])
                for fld007 in holding007s:
                    holding_record.add_field(Field(tag='007', data=fld007.data))
                for fld008 in holding008s:
                    holding_record.add_field(Field(tag='008', data=fld008.data))
                holding_records_by_id[subfield.value] = holding_record

        item_jsons = []
        items_by_holding_id = {}
        # move all the holding fields to the holding marc and create the items
        for field in record.fields.copy():
            if field.is_control_field():
                continue
            subfield = get_holding_id_subfield(field)
            if subfield:
                holding_record = holding_records_by_id[subfield.value]
                record.remove_field(field)
                field.subfields.remove(subfield)
                holding_record.add_field(field)
            if field.tag == item_field:
                if get_holding_id_subfield(field, '0'):
                    pids = items_by_holding_id.get(field['0'],[])
                    pids.append(field['2'])
                    items_by_holding_id[field['0']] = pids
                    json_string = field_to_item_json(item_template, mms_id, field)
                    item_jsons.append(json_string)
                    record.remove_field(field)
                    cnt_items += 1
        if not mms_id in bib_ids:
            if callback and 'process_marc' in callback:
                callback['process_marc'](mms_id, record, bibs_holding_ids)
            cnt_bibs += 1
            bib_ids.add(mms_id)
        for holding_record in holding_records_by_id.values():
            holding_id = holding_record.get_fields('001')[0].value()
            if not holding_id in holding_ids:
                if callback and 'process_holding' in callback:
                    callback['process_holding'](mms_id, holding_id, holding_record, items_by_holding_id.get(holding_id,[]))
                cnt_holdings += 1
                holding_ids.add(holding_id)
        for item_json in item_jsons:
            if callback and 'process_item' in callback:
                callback['process_item'](item_json)
    except Exception as e:
        cnt_errors += 1
        logging.exception(f'Error processing MMSID: {mms_id} in {publish_file}, error: {e}')

def get_holding_id_subfield(field, code='8'):
    for subfield in field:
        if subfield.code == code and subfield.value.startswith(holding_prefix) and subfield.value.endswith(institution_id):
            return subfield
    return None

def parse_file(publish_file, item_template, callback_creator):
    try:
        callback = callback_creator()
    except Exception as e:
        logging.exception(f'{e}')
    start_time = time.time()
    if callback and 'message' in callback:
        callback['message'](f'parsing publish file {publish_file}')
    with open(publish_file, 'rb') as fh:
        tar = None
        fh1 = fh
        if publish_file.endswith('gz'):
            tar = tarfile.open(fileobj=fh, mode="r:gz")
            for member in tar.getmembers():
                f = tar.extractfile(member)
                if f is not None:
                    fh1 = f
        if publish_file.endswith('xml') or publish_file.endswith('gz'):
            handler = PymarcXmlHandler(item_template, callback, handle_record, publish_file)
            try:
                reader = parse_xml(fh1, handler)
            except Exception as e:
                logging.exception(f'Exception parsing, errors: {e}')
        else:
            cnt = 0
            reader = MARCReader(fh1)
            for record in reader:
                if record is None:

                    if callback and 'message' in callback:
                        callback['message']('Current chunk: ', reader.current_chunk, ' was ignored because the following exception raised: ', reader.current_exception)
                        callback['message']('Processing may stop pre-maturely because of marc record errors')
                else:
                    cnt += 1
                    handle_record(item_template,  bib_ids, holding_ids, callback, publish_file)
        if tar:
            tar.close()

    if callback and 'message' in callback:
        callback['message'](f'done parsing publish file {publish_file} in {(time.time() - start_time)} seconds')
    if callback and 'file_complete' in callback:
        callback['file_complete']()


def remove_prefix(text, prefix):
    if text.startswith(prefix):
        return text[len(prefix):]
    return text

def remove_suffix(text, suffix):
    if text.endswith(suffix):
        return text[:-len(suffix)]
    return text

def field_to_item_json(template, mms_id, field):
    json_string = template.replace('<mms_id>', mms_id)
    perm_library = None
    perm_location = None
    current_library = None
    current_location = None
    for subfield in field:
        json_escaped_value = remove_suffix(remove_prefix(json.dumps(subfield.value),'"'),'"')
        json_string = json_string.replace('<$' +subfield.code + '>', json_escaped_value)
        if subfield.code == 's':
            perm_location = json_escaped_value
        if subfield.code == 't':
            current_location = json_escaped_value
        if subfield.code == 'h':
            perm_library = json_escaped_value
        if subfield.code == 'i':
            current_library = json_escaped_value
    if perm_location == current_location and perm_library == current_library:
        in_temp_location = 'false'
    else:
        in_temp_location = 'true'
    json_string = json_string.replace('"<in_temp_location>"', in_temp_location)
    # delete any remaining subfields in the template
    json_string = subfield_str_removal.sub('', json_string)
    return json_string

def is_glob(filename):
    return '*' in filename

def process_publish_marc(publish_files, max_workers, callback_creator):
    global cnt_files
    start_time = time.time()
    item_template = load_item_template()
    if is_glob(publish_files):
        all_files = glob.glob(publish_files)
        all_files.sort()
        for file_set in group_files(all_files):
            process_files(max_workers, callback_creator, item_template, file_set)
    elif os.path.isfile(publish_files):
        cnt_files += 1
        if "delete" in publish_files:
            process_delete_file(publish_files, callback_creator)
        else:
            parse_file(publish_files, item_template, callback_creator)
    elif os.path.isdir(publish_files):
            files = [os.path.join(publish_files, file) for file in os.listdir(publish_files)]
            process_files(max_workers, callback_creator, item_template, files)
    logger.info(f'Records:\t{cnt}\nBibs:   \t{cnt_bibs}\nHoldings:\t{cnt_holdings}\nItems:   \t{cnt_items}\nDeletes:   \t{cnt_deletes}\nErrors:   \t{cnt_errors}\nElapsed {(time.time() - start_time)} seconds')
    return {
        'files': cnt_files,
        'ingest': cnt,
        'error': cnt_errors,
        'delete': cnt_deletes
    }

def group_files(files):
    groups = []
    current_group = None
    current_dir = None
    for file in files:
        if os.path.dirname(file) != current_dir:
            if current_group:
                groups.append(current_group)
            current_group = []
            current_dir = os.path.dirname(file)
        current_group.append(file)
    if current_group:
        groups.append(current_group)
    return groups

def process_files(max_workers, callback_creator, item_template, files):
    global cnt_files
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        for file in files:
            cnt_files += 1
            if "delete" in file:
                process_delete_file(file, callback_creator)
                continue
            executor.submit(parse_file, file, item_template, callback_creator)
        executor.shutdown(wait=True)

def process_delete_file(publish_file, callback_creator):
    global cnt_deletes
    start_time = time.time()
    callback = callback_creator()
    if callback and 'message' in callback:
        callback['message'](f'parsing delete file {publish_file}')

    with open(publish_file, 'rb') as fh:
        tar = None
        fh1 = fh
        if publish_file.endswith('gz'):
            tar = tarfile.open(fileobj=fh, mode="r:gz")
            for member in tar.getmembers():
                f = tar.extractfile(member)
                if f is not None:
                    fh1 = f
        if publish_file.endswith('xml') or publish_file.endswith('gz'):
            deletedRecords = marcxml.parse_xml_to_array(fh1)
            for deleteRecord in deletedRecords:
                for field in deleteRecord.get_fields("852"):
                    holding_id_subfield = get_holding_id_subfield(field)
                    if holding_id_subfield:
                        holding_id = holding_id_subfield.value
                        # delete holding
                        if callback and 'delete_holding' in callback:
                            callback['delete_holding'](holding_id)
                        break
                fld001 = deleteRecord.get_fields('001')[0]
                mms_id = fld001.value()
                #trigger delete bib
                if callback and 'delete_bib' in callback:
                    cnt_deletes += 1
                    callback['delete_bib'](mms_id)
        if tar:
            tar.close()

    if callback and 'message' in callback:
        callback['message'](f'done parsing deleted file {publish_file} in {(time.time() - start_time)} seconds')
    if callback and 'file_complete' in callback:
        callback['file_complete']()

