import os

import agate
import agatesql  # noqa
import cchardet as chardet

from csvapi.utils import get_db_info

SNIFF_LIMIT = 4096


def is_binary(filepath):
    with os.popen('file {} -b --mime-type'.format(filepath)) as proc:
        return 'text/plain' not in proc.read().lower()


def detect_encoding(filepath):
    with open(filepath, 'rb') as f:
        return chardet.detect(f.read()).get('encoding')


def from_csv(filepath, encoding='utf-8', sniff_limit=SNIFF_LIMIT):
    """Try first w/o sniffing and then sniff if it fails"""
    try:
        return agate.Table.from_csv(filepath, sniff_limit=sniff_limit, encoding=encoding)
    except ValueError:
        return agate.Table.from_csv(filepath, encoding=encoding)


def from_excel(filepath):
    import agateexcel  # noqa
    return agate.Table.from_xls(filepath)


def to_sql(table, urlhash, storage):
    db_info = get_db_info(urlhash, storage=storage)
    table.to_sql(db_info['dsn'], db_info['db_name'], overwrite=True)


def parse(filepath, urlhash, storage, encoding=None, sniff_limit=SNIFF_LIMIT):
    if is_binary(filepath):
        table = from_excel(filepath)
    else:
        encoding = detect_encoding(filepath) if not encoding else encoding
        table = from_csv(filepath, encoding=encoding, sniff_limit=sniff_limit)
    return to_sql(table, urlhash, storage)
