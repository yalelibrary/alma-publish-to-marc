from dateutil.parser import parse as parse_date
import re
from dateutil.tz import gettz
import io
from pymarc import marcxml
from xml.etree import ElementTree as ET

trailing_punct = re.compile(' *[,\\/;:] *$')
trailing_period = re.compile('( *[^\\W\\d]{3,})\\.$')
trailing_bracket = re.compile('\\A\\[?([^\\[\\]]+)\\]?\\Z')

def subfields_as_string(record, field_tag, codes, join_with = ' '):
    fields = record.get_fields(field_tag)
    for field in fields:
        subfields = [subfield.value for subfield in field.subfields if subfield.code in codes or codes == '*']
        if subfields:
            return join_with.join(subfields)
    return None

# 245abfghknp
def extract_title(record):
    return trim_punctuation(subfields_as_string(record, '245', 'abfghknp'))

# 100:110:111
def extract_author(record):
    fields = record.get_fields('100', '110', '111')
    values = []
    for field in fields:
        values.extend([subfield.value for subfield in field.subfields if subfield.code != '0'])
    return trim_punctuation(' '.join(values))

# 260c:264|*1|c:264|*0|c:264|*2|c:264|*3|c:260g
def extract_publication_date(record):
    values = []
    for field in record.get_fields('260'):
        if field.get_subfields('c'):
            values.append(' '.join(field.get_subfields('c')))
    for field in record.get_fields('264'):
        if field.indicator2 in ['1', '2',' 3'] and field.get_subfields('c'):
            values.append(' '.join(field.get_subfields('c')))
    for field in record.get_fields('260'):
        if field.get_subfields('g'):
            values.append(' '.join(field.get_subfields('g')))
    return trim_punctuation(' '.join(values))[:254]

# 260bf:264b
def extract_publisher(record):
    values = []
    for field in record.get_fields('260'):
        if field.get_subfields('b', 'f'):
            values.append(' '.join(field.get_subfields('b', 'f')))
    for field in record.get_fields('264'):
        if field.get_subfields('b'):
            values.append(' '.join(field.get_subfields('b')))
    return trim_punctuation(' '.join(values))

# 260ae:264a:752abcd
def extract_publication_place(record):
    values = []
    for field in record.get_fields('260'):
        if field.get_subfields('a', 'e'):
            values.append(' '.join(field.get_subfields('a', 'e')))
    for field in record.get_fields('264'):
        if field.get_subfields('a'):
            values.append(' '.join(field.get_subfields('a')))
    for field in record.get_fields('752'):
        if field.get_subfields('a', 'b', 'c', 'd'):
            values.append(' '.join(field.get_subfields('a', 'b', 'c', 'd')))
    return trim_punctuation(' '.join(values))

# 300acef
def extract_extent(record):
    return subfields_as_string(record, '300', 'acef')

# 300b:340
def extract_material(record):
    x = []
    x.append(subfields_as_string(record, '300', 'b'))
    x.append(subfields_as_string(record, '340', '*'))
    return ' '.join([v for v in x if v])

# 852khimt
def extract_call_number(record):
    return trim_punctuation(subfields_as_string(record, '852', 'khimt'))

def extract_fixed_field(record, tag):
    fields = record.get_fields(tag)
    for field in fields:
        return field.data
    return None

# 035$a
def extract_voyager_or_sierra_id(record):
    fields = record.get_fields('035')
    for field in fields:
        if field.get_subfields('a'):
            value = field.get_subfields('a')[0]
            if value.startswith('(CtY-L)b'):
                return value.split('(CtY-L)',1)[1][:-1]
            if value.startswith('(CtY)') and value.endswith('-yaledb-Voyager'):
                return value.split('(CtY)',1)[1][:-len('-yaledb-Voyager')]
    return None


# 856 b and c
def extract_library_and_location_code(record):
    fields = record.get_fields('852')
    for field in fields:
        b = field.get_subfields('b')
        c = field.get_subfields('c')
        if b and c:
            return (b[0], c[0])
    return None


def extract_isbns(record):
    values = extract_values(record, '020', 'z')
    best_value = None
    if values:
        for value in values:
            if len(value) >= 13:
                best_value = value
                break
        if best_value:
            values.insert(0, values.pop(values.index(best_value)))
        return values
    return None

def extract_issns(record):
    return extract_values(record, '022', 'yz')

def extract_oclcs(record):
    oclcs = []
    for field in record.get_fields('035', '079'):
        if field.get_subfields('z'):
            continue
        suba = ' '.join(field.get_subfields('a'))
        if suba:
            suba_lower = suba.lower()
            if suba_lower.startswith('(ocolc)') or suba_lower.startswith('oc'):
                oclcs.append(suba)
    return oclcs


def extract_values(record, field_tag, skip_subfields):
    for field in record.get_fields(field_tag):
        if [sf for sf in field.subfields if sf.code in skip_subfields]:
            continue
        if field.get_subfields('a'):
            return field.get_subfields('a')
    return None

def extract_system_dates(record, field_tag):
    for field in record.get_fields(field_tag):
        c = field.get_subfields('1')
        d = field.get_subfields('2') or c
        if c and d:
            return (parse_date_str(c[0]), parse_date_str(d[0]))
    return None

def parse_date_str(date_string):
    if not date_string:
        return None
    date_string = date_string.replace('US/Eastern', 'EST')
    tzinfos = {"EST": gettz("America/New_York")}
    return parse_date(date_string, tzinfos=tzinfos)


def recursive_sub(regex, replace, string):
    while True:
        output = regex.sub(replace, string)
        if output == string:
            break
        string = output
    return string

def trim_punctuation(string):
    if not string:
        return string
    string = recursive_sub(trailing_punct,'', string)
    string = recursive_sub(trailing_period, '\\1', string)
    string = recursive_sub(trailing_bracket, '\\1', string)
    string = string.strip()
    if string == '.':
        string = ''
    return string


def to_marc_xml(record):
    return marcxml.record_to_xml(record).decode('UTF-8')