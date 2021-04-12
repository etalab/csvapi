import hashlib

from pathlib import Path

from quart import current_app as app

executor = None


def get_db_info(urlhash, storage=None):
    # app.config not thread safe, sometimes we need to pass storage directly
    db_storage = storage or app.config['DB_ROOT_DIR']
    profile_storage = app.config['PROFILES_ROOT_DIR']
    db_path = f"{db_storage}/{urlhash}.db"
    profile_path = f"{profile_storage}/{urlhash}.html"
    return {
        'dsn': f"sqlite:///{db_path}",
        'db_name': urlhash,
        'table_name': urlhash,
        'db_path': db_path,
        'profile_path': profile_path,
    }


def get_hash(to_hash):
    return get_hash_bytes(to_hash.encode('utf-8'))


def get_hash_bytes(to_hash):
    return hashlib.md5(to_hash).hexdigest()


def already_exists(urlhash):
    cache_enabled = app.config.get('CSV_CACHE_ENABLED')
    if not cache_enabled:
        return False
    return Path(get_db_info(urlhash)['db_path']).exists()
