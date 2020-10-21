import os

import agate
import agatesql  # noqa
import cchardet as chardet

from csvapi.utils import get_db_info
from csvapi.type_tester import agate_tester

SNIFF_LIMIT = 4096


def is_csv(filepath):
    with os.popen('file {} -b --mime-type'.format(filepath)) as proc:
        return 'text/plain' in proc.read().lower()


def is_xls(filepath):
    with os.popen('file {} -b --mime-type'.format(filepath)) as proc:
        return 'application/vnd.ms-excel' in proc.read().lower()


def is_xlsx(filepath):
    with os.popen('file {} -b --mime-type'.format(filepath)) as proc:
        return 'application/vnd.openxml' in proc.read().lower()


def detect_encoding(filepath):
    with open(filepath, 'rb') as f:
        return chardet.detect(f.read()).get('encoding')


def from_csv(filepath, encoding='utf-8', sniff_limit=SNIFF_LIMIT):
    """Try first w/ sniffing and then w/o sniffing if it fails"""
    try:
        return agate.Table.from_csv(filepath, sniff_limit=sniff_limit, encoding=encoding, column_types=agate_tester())
    except ValueError:
        return agate.Table.from_csv(filepath, encoding=encoding, column_types=agate_tester())


def from_excel(filepath, xlsx=False):
    import agateexcel  # noqa
    if xlsx:
        return agate.Table.from_xlsx(filepath, column_types=agate_tester())
    return agate.Table.from_xls(filepath, column_types=agate_tester())


def to_sql(table, urlhash, storage):
    db_info = get_db_info(urlhash, storage=storage)
    table.to_sql(db_info['dsn'], db_info['db_name'], overwrite=True)


def parse(filepath, urlhash, storage, encoding=None, sniff_limit=SNIFF_LIMIT):
    if is_xls(filepath):
        print('XLS FILE')
        table = from_excel(filepath)
    elif is_xlsx(filepath):
        print('XLSX FILE')
        table = from_excel(filepath, xlsx=True)
    elif is_csv(filepath):
        print('CSV FILE')
        encoding = detect_encoding(filepath) if not encoding else encoding
        table = from_csv(filepath, encoding=encoding, sniff_limit=sniff_limit)
    else:
        raise Exception('Unsupported file type')
    return to_sql(table, urlhash, storage)
