import aiosqlite
import sqlite3
import time

from contextlib import asynccontextmanager
from pathlib import Path

from quart import request, jsonify, current_app as app
from quart.views import MethodView
from slugify import slugify

from csvapi.errors import APIError
from csvapi.utils import get_db_info

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

    async def execute(self, sql, db_info, params=None):
        """Executes sql against db_name in a thread"""
        dsn = 'file:{}?immutable=1'.format(db_info['db_path'])
        # specify uri=True to make sure `file:xxx` is supported,
        # however the backend sqlite is configured (eg default MacOS)
        async with aiosqlite.connect(dsn, uri=True) as conn:
            conn.text_factory = lambda x: str(x, 'utf-8', 'replace')
            # this will raise
            #  {"details": "interrupted",
            #  "error": "Error selecting data",}
            async with sqlite_timelimit(conn, SQL_TIME_LIMIT_MS):
                try:
                    async with conn.execute(sql, params or {}) as cursor:
                        rows = await cursor.fetchall()
                except Exception:
                    app.logger.error('ERROR: conn={}, sql = {}, params = {}'.format(
                        conn, repr(sql), params
                    ))
                    raise
            return rows, cursor.description

    def add_filters_to_sql(self, sql, filters):
        wheres = []
        params = {}
        for (f_key, f_value) in filters:
            comparator = f_key.split('__')[1]
            column = f_key.split('__')[0]
            normalized_column = slugify(column, separator='_')
            if comparator == 'exact':
                wheres.append(f"[{column}] = :filter_value_{normalized_column}")
                params[f'filter_value_{normalized_column}'] = f_value
            elif comparator == 'contains':
                wheres.append(f"[{column}] LIKE :filter_value_{normalized_column}")
                params[f'filter_value_{normalized_column}'] = f'%{f_value}%'
            else:
                app.logger.warning(f'Dropped unknown comparator in {f_key}')
        if wheres:
            sql += ' WHERE '
            sql += ' AND '.join(wheres)
        return sql, params

    async def data(self, db_info, export=False):
        limit = request.args.get('_size', ROWS_LIMIT) if not export else -1
        rowid = not (request.args.get('_rowid') == 'hide') and not export
        total = not (request.args.get('_total') == 'hide') and not export
        sort = request.args.get('_sort')
        sort_desc = request.args.get('_sort_desc')
        offset = request.args.get('_offset') if not export else 0

        # get filter arguments, like column__exact=xxx
        filters = []
        for key, value in request.args.items():
            if not key.startswith('_') and '__' in key:
                filters.append((key, value))

        cols = 'rowid, *' if rowid else '*'
        sql = 'SELECT {} FROM [{}]'.format(cols, db_info['table_name'])
        sql, params = self.add_filters_to_sql(sql, filters)
        if sort:
            sql += f' ORDER BY [{sort}]'
        elif sort_desc:
            sql += f' ORDER BY [{sort_desc}] DESC'
        else:
            sql += ' ORDER BY rowid'
        sql += ' LIMIT :l'
        params['l'] = limit
        if offset:
            sql += ' OFFSET :o'
            params['o'] = offset
        rows, description = await self.execute(
            sql, db_info, params=params
        )

        columns = [r[0] for r in description]

        if export:
            return columns, rows

        res = {
            'columns': columns,
            'rows': list(rows),
        }

        if total:
            sql = f"SELECT COUNT(*) FROM [{db_info['table_name']}]"
            sql, params = self.add_filters_to_sql(sql, filters)
            r, _ = await self.execute(sql, db_info, params=params)
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

        general_infos = await self.general_infos(db_info)
        columns_infos = await self.columns_infos(db_info)

        res = {
            'ok': True,
            'query_ms': (end - start) * 1000,
            'rows': rows,
            'columns': data['columns'],
            'general_infos': general_infos,
            'columns_infos': columns_infos
        }
        if data.get('total'):
            res['total'] = data['total']

        return jsonify(res)

    async def general_infos(self, db_info):
        params = {}
        sql = 'SELECT count(*) FROM sqlite_master WHERE type=\'table\' AND name=\'general_infos\''
        rows, description = await self.execute(
            sql, db_info, params=params
        )
        if rows[0][0] != 0:
            sql = 'SELECT * FROM general_infos'
            rows, description = await self.execute(
                sql, db_info, params=params
            )
            columns = [r[0] for r in description]
            res = {}
            cpt = 0
            for col in columns:
                res[col] = rows[0][cpt]
                cpt = cpt + 1

            return res
        else:
            return {}

    async def columns_infos(self, db_info):
        params = {}
        sql = 'SELECT count(*) FROM sqlite_master WHERE type=\'table\' AND name=\'columns_infos\''
        rows, description = await self.execute(
            sql, db_info, params=params
        )
        if rows[0][0] != 0:
            sql = 'SELECT * FROM columns_infos'
            rows, description = await self.execute(
                sql, db_info, params=params
            )
            columns = [r[0] for r in description]

            res = {}
            for row in rows:
                cpt = 1
                res[row[0]] = {}
                for col in columns[1:]:
                    res[row[0]][col] = row[cpt]
                    cpt = cpt + 1

            res = await self.top_and_categorical_infos(db_info, res, 'top_infos')
            res = await self.top_and_categorical_infos(db_info, res, 'categorical_infos')
            res = await self.numeric_infos(db_info, res)
            res = await self.numeric_plot_infos(db_info, res)
            return res
        else:
            return {}

    async def top_and_categorical_infos(self, db_info, res, table_name):
        params = {}
        sql = 'SELECT count(*) FROM sqlite_master WHERE type=\'table\' AND name=\'{}\''.format(table_name)
        rows, description = await self.execute(
            sql, db_info, params=params
        )
        if rows[0][0] != 0:
            sql = 'SELECT * FROM {}'.format(table_name)
            rows, description = await self.execute(
                sql, db_info, params=params
            )

            for row in rows:
                if table_name not in res[row[0]]:
                    res[row[0]][table_name] = []
                inter = {}
                inter['value'] = row[1]
                inter['count'] = row[2]
                res[row[0]][table_name].append(inter)

            return res
        else:
            for col in res:
                res[col][table_name] = {}
            return res

    async def numeric_infos(self, db_info, res):
        params = {}
        sql = 'SELECT count(*) FROM sqlite_master WHERE type=\'table\' AND name=\'numeric_infos\''
        rows, description = await self.execute(
            sql, db_info, params=params
        )
        if rows[0][0] != 0:
            sql = 'SELECT * FROM {}'.format('numeric_infos')
            rows, description = await self.execute(
                sql, db_info, params=params
            )

            for row in rows:
                if 'numeric_infos' not in res[row[0]]:
                    res[row[0]]['numeric_infos'] = {}

                res[row[0]]['numeric_infos']['mean'] = row[1]
                res[row[0]]['numeric_infos']['std'] = row[2]
                res[row[0]]['numeric_infos']['min'] = row[3]
                res[row[0]]['numeric_infos']['max'] = row[4]

            return res
        else:
            for col in res:
                res[col]['numeric_infos'] = {}
            return res

    async def numeric_plot_infos(self, db_info, res):
        params = {}
        sql = 'SELECT count(*) FROM sqlite_master WHERE type=\'table\' AND name=\'numeric_plot_infos\''
        rows, description = await self.execute(
            sql, db_info, params=params
        )
        if rows[0][0] != 0:
            sql = 'SELECT * FROM {}'.format('numeric_plot_infos')
            rows, description = await self.execute(
                sql, db_info, params=params
            )

            for row in rows:
                if 'numeric_plot_infos' not in res[row[0]]:
                    res[row[0]]['numeric_plot_infos'] = {}
                if 'counts' not in res[row[0]]['numeric_plot_infos']:
                    res[row[0]]['numeric_plot_infos']['counts'] = []
                if 'bin_edges' not in res[row[0]]['numeric_plot_infos']:
                    res[row[0]]['numeric_plot_infos']['bin_edges'] = []
                if row[2] == 'counts':
                    res[row[0]]['numeric_plot_infos']['counts'].append(row[1])
                if row[2] == 'bin_edges':
                    res[row[0]]['numeric_plot_infos']['bin_edges'].append(row[1])

            return res
        else:
            for col in res:
                res[col]['numeric_plot_infos'] = {}
            return res
