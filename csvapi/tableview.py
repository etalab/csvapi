import asyncio
import sqlite3
import time

import aiosqlite

from contextlib import asynccontextmanager
from pathlib import Path


from quart import request, jsonify, current_app as app
from quart.views import MethodView

from csvapi.errors import APIError
from csvapi.utils import get_db_info

loop = asyncio.get_event_loop()

ROWS_LIMIT = 100
SQL_TIME_LIMIT_MS = 1000
DEFAULT_SHAPE = 'lists'


def prepare_connection(conn):
    # conn.row_factory = sqlite3.Row
    conn.text_factory = lambda x: str(x, 'utf-8', 'replace')


@asynccontextmanager
async def sqlite_timelimit(conn, ms):
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

    await conn.set_progress_handler(handler, n)
    yield
    await conn.set_progress_handler(None, n)


class TableView(MethodView):

    async def options(self):
        pass

    async def execute(self, sql, db_info, params=None):
        async with aiosqlite.connect(f"file:{db_info['db_path']}?immutable=1", uri=True) as conn:
            prepare_connection(conn)
            try:
                async with sqlite_timelimit(conn, SQL_TIME_LIMIT_MS):
                    async with conn.execute(sql, params or {}) as cursor:
                        rows = await cursor.fetchall()
                        return rows, cursor.description
            except Exception:
                app.logger.error(f"ERROR: conn={conn}, sql = {repr(sql)}, params = {params}")
                raise

    async def data(self, db_info):
        limit = request.args.get('_size', ROWS_LIMIT)
        rowid = not (request.args.get('_rowid') == 'hide')
        total = not (request.args.get('_total') == 'hide')
        sort = request.args.get('_sort')
        sort_desc = request.args.get('_sort_desc')
        offset = request.args.get('_offset')

        cols = 'rowid, *' if rowid else '*'
        sql = f"SELECT {cols} FROM [{db_info['table_name']}]"
        if sort:
            sql += f" ORDER BY [{sort}]"
        elif sort_desc:
            sql += f" ORDER BY [{sort_desc}] DESC"
        else:
            sql += ' ORDER BY rowid'
        sql += ' LIMIT :l'
        if offset:
            sql += ' OFFSET :o'
        rows, description = await self.execute(
            sql, db_info, params={'l': limit, 'o': offset}
        )
        columns = [r[0] for r in description]
        res = {
            'columns': columns,
            'rows': list(rows),
        }

        if total:
            r, d = await self.execute(
                f"SELECT COUNT(*) FROM [{db_info['table_name']}]",
                db_info
            )
            res['total'] = r[0][0]

        return res

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
            raise APIError(f"Unknown _shape: {_shape}", status=400)

        res = {
            'ok': True,
            'query_ms': (end - start) * 1000,
            'rows': rows,
            'columns': data['columns'],
        }
        if data.get('total'):
            res['total'] = data['total']

        return jsonify(res)
