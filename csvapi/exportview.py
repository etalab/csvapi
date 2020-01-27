import csv
import sqlite3

from io import StringIO
from pathlib import Path

from quart import make_response

from csvapi.errors import APIError
from csvapi.tableview import TableView
from csvapi.utils import get_db_info


class ExportView(TableView):

    async def get(self, urlhash):
        "This will inherit sorting and filtering from TableView"
        db_info = get_db_info(urlhash)
        p = Path(db_info['db_path'])
        if not p.exists():
            raise APIError('Database has probably been removed.', status=404)

        try:
            columns, rows_iter = await self.data(db_info, export=True)
        except (sqlite3.OperationalError, sqlite3.IntegrityError) as e:
            raise APIError('Error selecting data', status=400, payload=dict(details=str(e)))

        def make_line(line_data):
            line = StringIO()
            writer = csv.writer(line)
            writer.writerow(line_data)
            line.seek(0)
            return line.read().encode()

        async def _make_response():
            yield make_line(columns)
            for line in rows_iter:
                yield make_line(line)

        response = await make_response(_make_response())
        response.mimetype = 'text/csv'
        response.headers['Content-Disposition'] = f'attachment; filename={urlhash}.csv'
        return response
