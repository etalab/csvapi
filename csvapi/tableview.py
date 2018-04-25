import sqlite3
import time

from contextlib import contextmanager
from concurrent import futures
from pathlib import Path

import asyncio
import threading

from sanic import response
from sanic.exceptions import abort
from sanic.views import HTTPMethodView

from csvapi.utils import get_db_info

connections = threading.local()
# XXX is this a right place?
executor = futures.ThreadPoolExecutor(max_workers=3)

ROWS_LIMIT = 100
SQL_TIME_LIMIT_MS = 1000
DEFAULT_SHAPE = 'compact'


def prepare_connection(conn):
    conn.row_factory = sqlite3.Row
    conn.text_factory = lambda x: str(x, 'utf-8', 'replace')


@contextmanager
def sqlite_timelimit(conn, ms):
    deadline = time.time() + (ms / 1000)
    # n is the number of SQLite virtual machine instructions that will be
    # executed between each check. It's hard to know what to pick here.
    # After some experimentation, I've decided to go with 1000 by default and
    # 1 for time limits that are less than 50ms
    n = 1000
    if ms < 50:
        n = 1

    def handler():
        if time.time() >= deadline:
            return 1

    conn.set_progress_handler(handler, n)
    yield
    conn.set_progress_handler(None, n)


class TableView(HTTPMethodView):

    async def execute(self, sql, db_info, params=None):
        """Executes sql against db_name in a thread"""
        def sql_operation_in_thread():
            conn = getattr(connections, db_info['db_name'], None)
            if not conn:
                conn = sqlite3.connect(
                    'file:{}?immutable=1'.format(db_info['db_path']),
                    uri=True,
                    check_same_thread=False,
                )
                prepare_connection(conn)
                setattr(connections, db_info['db_name'], conn)

            with sqlite_timelimit(conn, SQL_TIME_LIMIT_MS):
                try:
                    cursor = conn.cursor()
                    cursor.execute(sql, params or {})
                    rows = cursor.fetchall()
                except Exception:
                    print('ERROR: conn={}, sql = {}, params = {}'.format(
                        conn, repr(sql), params
                    ))
                    raise
            return rows, cursor.description

        return await asyncio.get_event_loop().run_in_executor(
            executor, sql_operation_in_thread
        )

    async def data(self, request, db_info):
        limit = request.args.get('limit', ROWS_LIMIT)
        sql = 'SELECT rowid, * FROM "{t}" ORDER BY rowid LIMIT {l}'
        sql = sql.format(**{
            't': db_info['table_name'],
            'l': limit,
        })
        rows, description = await self.execute(sql, db_info)
        columns = [r[0] for r in description]
        return {
            'columns': columns,
            'rows': list(rows),
        }

    async def get(self, request, _hash):
        db_info = get_db_info(
            request.app.config.get('DB_ROOT_DIR'),
            _hash
        )
        p = Path(db_info['db_path'])
        if not p.exists():
            abort(404, 'Database has probably been removed.')
        start = time.time()
        try:
            data = await self.data(request, db_info)
        except (sqlite3.OperationalError) as e:
            data = {
                'ok': False,
                'error': str(e),
            }
            abort(400, data)
        end = time.time()

        _shape = request.args.get('_shape', DEFAULT_SHAPE)
        if _shape == 'objects':
            # Format data as an array of objects for the client
            rows = []
            for row in data['rows']:
                rows.append(dict(zip(data['columns'], row)))
        elif _shape == 'compact':
            rows = data['rows']

        return response.json({
            'ok': True,
            'query_ms': (end - start) * 1000,
            'rows': rows,
            'columns': data['columns'],
        }, headers={'Access-Control-Allow-Origin': '*'})
