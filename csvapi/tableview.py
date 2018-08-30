import asyncio
import sqlite3
import threading
import time

from contextlib import contextmanager
from pathlib import Path


from quart import request, jsonify, current_app as app
from quart.views import MethodView

from csvapi.errors import APIError
from csvapi.utils import get_db_info, get_executor

connections = threading.local()

ROWS_LIMIT = 100
SQL_TIME_LIMIT_MS = 1000
DEFAULT_SHAPE = 'lists'


def prepare_connection(conn):
    # conn.row_factory = sqlite3.Row
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


class TableView(MethodView):

    async def options(self):
        pass

    async def execute(self, sql, db_info, params=None):
        """Executes sql against db_name in a thread"""
        def sql_operation_in_thread(logger):
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
                    logger.error('ERROR: conn={}, sql = {}, params = {}'.format(
                        conn, repr(sql), params
                    ))
                    raise
            return rows, cursor.description

        return await asyncio.get_event_loop().run_in_executor(
            get_executor(), sql_operation_in_thread, app.logger
        )

    async def data(self, db_info, rowid=True):
        limit = request.args.get('_size', ROWS_LIMIT)
        rowid = not (request.args.get('_rowid') == 'hide')
        sort = request.args.get('_sort')
        sort_desc = request.args.get('_sort_desc')
        offset = request.args.get('_offset')

        cols = 'rowid, *' if rowid else '*'
        sql = 'SELECT {} FROM [{}]'.format(cols, db_info['table_name'])
        if sort:
            sql += ' ORDER BY [{}]'.format(sort)
        elif sort_desc:
            sql += ' ORDER BY [{}] DESC'.format(sort_desc)
        else:
            sql += ' ORDER BY rowid'
        sql += ' LIMIT :l'
        if offset:
            sql += ' OFFSET :o'
        rows, description = await self.execute(
            sql, db_info, params={'l': limit, 'o': offset}
        )
        columns = [r[0] for r in description]
        return {
            'columns': columns,
            'rows': list(rows),
        }

    async def get(self, urlhash):
        db_info = get_db_info(urlhash)
        p = Path(db_info['db_path'])
        if not p.exists():
            raise APIError('Database has probably been removed.', status=404)

        start = time.time()
        try:
            data = await self.data(db_info)
        except (sqlite3.OperationalError, sqlite3.IntegrityError) as e:
            raise APIError('Error selecting data', status=400, payload=dict(details=str(e)))
        end = time.time()

        _shape = request.args.get('_shape', DEFAULT_SHAPE)
        if _shape == 'objects':
            # Format data as an array of objects for the client
            rows = []
            for row in data['rows']:
                rows.append(dict(zip(data['columns'], row)))
        elif _shape == 'lists':
            rows = data['rows']
        else:
            raise APIError('Unknown _shape: {}'.format(_shape), status=400)

        return jsonify({
            'ok': True,
            'query_ms': (end - start) * 1000,
            'rows': rows,
            'columns': data['columns'],
        })
