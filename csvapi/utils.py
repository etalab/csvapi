import os
import datetime

import sqlite3
import xxhash

from concurrent import futures
from pathlib import Path

from quart import current_app as app

executor = None


def create_sys_db(app, storage=None):
    # We do not use rhe get_sys_db_info here because the call is made outside of the app context.
    storage = storage or app.config['DB_ROOT_DIR']
    dbpath = f"{storage}/sys.db"

    conn = sqlite3.connect(dbpath)
    with conn:
        conn.execute("CREATE TABLE IF NOT EXISTS csvapi_sys (id integer primary key, db_uuid text, urlhash text, filehash text, creation_time date)")
    conn.close()


def get_sys_db_info():
    storage = app.config['DB_ROOT_DIR']
    dbpath = f"{storage}/sys.db"
    return {
        'dsn': f"sqlite:///{dbpath}",
        'db_name': "sys.db",
        'table_name': "csvapi_sys",
        'db_path': dbpath,
    }

def add_entry_to_sys_db(uuid, urlhash, filehash):
    now = datetime.datetime.now()
    now_str = now.strftime('%Y-%m-%d')

    sys_db = get_sys_db_info()
    conn = sqlite3.connect(sys_db['db_path'])
    with conn:
        conn.execute("INSERT INTO csvapi_sys (db_uuid, urlhash, filehash, creation_time) values (?, ?, ?, ?)", (uuid, urlhash, filehash, now_str))
    conn.close()



def get_db_info(urlhash, storage=None):
    storage = storage or app.config['DB_ROOT_DIR']

    sys_db = get_sys_db_info()
    conn = sqlite3.connect(sys_db['db_path'])
    c = conn.cursor()
    t = (urlhash,)
    c.execute('SELECT * FROM csvapi_sys WHERE urlhash=?', t)
    res = c.fetchone()
    if not res:
        return None

    dbuuid = res[1]
    urlhash = res[2]
    filehash = res[3]
    creadate = res[4]
    dbpath = f"{storage}/{dbuuid}"
    dbname = dbuuid

    conn.close()
    return {
        'db_uuid': dbuuid,
        'urlhash': urlhash,
        'filehash': filehash,
        'creation_date': creadate,
        'table_name': urlhash,
        'db_path': dbpath,
        'db_name': dbname,
        'dsn': f"sqlite:///{dbpath}"

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


def already_exists(urlhash):
    pass


def is_hash_relevant(urlhash, filehash):
    cache_enabled = app.config.get('CSV_CACHE_ENABLED')
    if not cache_enabled:
        return False

    db = get_db_info(urlhash)
    if db is None:
        return False

    if db['filehash'] == filehash:
        return True

    return False


def age_valid(storage, urlhash):
    max_age = app.config['CACHE_MAX_AGE']

    db = get_db_info(urlhash)
    if db is None:
        return False

    date_time_obj = datetime.datetime.strptime(db['creation_date'], '%Y-%m-%d')
    later_time = datetime.datetime.now()
    file_age = later_time - date_time_obj
    if file_age.days > max_age:
        return False

    return True



