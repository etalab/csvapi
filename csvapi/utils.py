import hashlib

from concurrent import futures

from quart import current_app as app

executor = None


def get_db_info(db_root_dir, _hash):
    dbpath = '{}/{}.db'.format(db_root_dir, _hash)
    return {
        'dsn': 'sqlite:///{}'.format(dbpath),
        'db_name': _hash,
        'table_name': _hash,
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
