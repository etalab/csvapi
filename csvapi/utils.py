from sanic import response


def get_db_info(db_root_dir, _hash):
    dbpath = '{}/{}.db'.format(db_root_dir, _hash)
    return {
        'dsn': 'sqlite:///{}'.format(dbpath),
        'db_name': _hash,
        'table_name': _hash,
        'db_path': dbpath,
    }


def api_error(msg, status=500, details=None):
    return response.json({
        'ok': False,
        'error': msg,
        'details': details,
    }, status)
