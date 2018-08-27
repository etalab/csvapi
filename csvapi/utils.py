import traceback

from sanic import response
from sanic.log import logger as log


def get_db_info(db_root_dir, _hash):
    dbpath = '{}/{}.db'.format(db_root_dir, _hash)
    return {
        'dsn': 'sqlite:///{}'.format(dbpath),
        'db_name': _hash,
        'table_name': _hash,
        'db_path': dbpath,
    }


def api_error(msg, status=500, details=None):
    log.error('api_error %s: %s (%s)', status, msg, details)
    return response.json({
        'ok': False,
        'error': msg,
        'details': details,
    }, status)


def api_error_from_e(msg, exception):
    traceback.print_exc()
    return api_error(msg, details=str(exception))
