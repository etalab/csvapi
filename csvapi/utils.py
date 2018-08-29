import hashlib

from concurrent import futures
from pathlib import Path

from quart import current_app as app

executor = None


def get_db_info(urlhash, storage=None):
    # app.config not thread safe, sometimes we need to pass storage directly
    storage = storage or app.config['DB_ROOT_DIR']
    dbpath = '{}/{}.db'.format(storage, urlhash)
    return {
        'dsn': 'sqlite:///{}'.format(dbpath),
        'db_name': urlhash,
        'table_name': urlhash,
        'db_path': dbpath,
    }


def get_executor():
    global executor
    if not executor:
        app.logger.debug('* Creating executor')
        max_workers = app.config.get('MAX_WORKERS')
        executor = futures.ThreadPoolExecutor(max_workers=max_workers)
    return executor


def get_hash(to_hash):
    return hashlib.md5(to_hash.encode('utf-8')).hexdigest()


def already_exists(urlhash):
    cache_enabled = app.config.get('CSV_CACHE_ENABLED')
    if not cache_enabled:
        return False
    return Path(get_db_info(urlhash)['db_path']).exists()
