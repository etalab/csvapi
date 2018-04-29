from concurrent import futures

from quart import jsonify, g, current_app


def get_db_info(db_root_dir, _hash):
    dbpath = '{}/{}.db'.format(db_root_dir, _hash)
    return {
        'dsn': 'sqlite:///{}'.format(dbpath),
        'db_name': _hash,
        'table_name': _hash,
        'db_path': dbpath,
    }


def api_error(msg, status=500, details=None):
    return jsonify({
        'ok': False,
        'error': msg,
        'details': details,
    }, status)


def get_executor():
    if not hasattr(g, 'executor'):
        max_workers = current_app.config.get('MAX_WORKERS')
        g.executor = futures.ThreadPoolExecutor(max_workers=max_workers)
    return g.executor
