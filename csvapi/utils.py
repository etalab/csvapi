import os
import datetime

import xxhash

from concurrent import futures
from pathlib import Path

from quart import current_app as app

executor = None


def get_db_info(urlhash, storage=None):
    # app.config not thread safe, sometimes we need to pass storage directly
    storage = storage or app.config['DB_ROOT_DIR']
    for f in os.listdir(storage):
        if f.startswith(urlhash):
            dbpath = f"{storage}/{f}"
            break
    return {
        'dsn': f"sqlite:///{dbpath}",
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
    return get_hash_bytes(to_hash.encode('utf-8'))


def get_hash_bytes(to_hash):
    return xxhash.xxh64(to_hash).hexdigest()


def already_exists(filehash):
    cache_enabled = app.config.get('CSV_CACHE_ENABLED')
    if not cache_enabled:
        return False
    return Path(get_db_info(filehash)['db_path']).exists()


def max_age(storage, urlhash):
    max_age = app.config['CACHE_MAX_AGE']
    for f in os.listdir(storage):
        if f.startswith(urlhash):
            mod_time = os.stat(f).st_mtime
            mod_timestamp = datetime.datetime.fromtimestamp(mod_time)
            later_time = datetime.datetime.now()
            file_age = later_time - mod_timestamp
            if file_age.days > max_age:
                return True
            else:
                break
    return False



