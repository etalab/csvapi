import agate
import agatesql  # noqa

from csvapi.utils import get_db_info

SNIFF_LIMIT = 2048


def csv_to_sql(filename, _hash, storage):
    table = agate.Table.from_csv(
        filename,
        sniff_limit=SNIFF_LIMIT,
    )
    db_info = get_db_info(storage, _hash)
    table.to_sql(db_info['dsn'], db_info['db_name'], overwrite=True)


async def parse(filename, _hash, storage='.'):
    encoding = 'utf-8'
    try:
        infile = open(filename)
        infile.read(SNIFF_LIMIT)
    except UnicodeDecodeError:
        infile.close()
        encoding = 'latin1'
        infile = open(filename, encoding=encoding)
        infile.read(SNIFF_LIMIT)
    infile.seek(0)
    try:
        return csv_to_sql(infile, _hash, storage)
    finally:
        infile.close()
