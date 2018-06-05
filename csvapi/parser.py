import os
import logging

from itertools import islice

import agate
import agatesql  # noqa

from csvapi.utils import get_db_info

log = logging.getLogger('__name__')

SNIFF_LIMIT = 2048
MAX_PREPARSE_LINES = 50


def is_binary(filepath):
    with os.popen('file {} -b --mime-type'.format(filepath)) as proc:
        return 'text/plain' not in proc.read().lower()


def detect_encoding(filepath):
    with os.popen('file {} -b --mime-encoding'.format(filepath)) as proc:
        return proc.read().replace('\n', '')


def from_csv(filepath, **agate_params):
    return agate.Table.from_csv(filepath, **agate_params)


def from_excel(filepath):
    import agateexcel  # noqa
    return agate.Table.from_xls(filepath)


def to_sql(table, _hash, storage):
    db_info = get_db_info(storage, _hash)
    table.to_sql(db_info['dsn'], db_info['db_name'], overwrite=True)


def parse(filepath, _hash, storage='.', parse_module=None):
    if is_binary(filepath):
        table = from_excel(filepath)
    else:
        encoding = detect_encoding(filepath)
        agate_params = {
            'encoding': encoding,
            'sniff_limit': SNIFF_LIMIT,
        }
        # TODO exception here do not bubble up to parseview.py :thinking:
        if parse_module:
            with open(filepath, encoding=encoding) as f:
                try:
                    pm = __import__(parse_module)
                except ModuleNotFoundError:
                    log.warning('Pre-parse module "{}" not found'.format(parse_module))
                else:
                    delimiter, skip_lines = pm.parse_csv(list(islice(f, MAX_PREPARSE_LINES)))
                    agate_params.update({
                        'delimiter': delimiter,
                        'skip_lines': skip_lines,
                    })
        table = from_csv(filepath, **agate_params)
    return to_sql(table, _hash, storage)
