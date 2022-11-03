from pathlib import Path

import pandas as pd
import sqlite3

from pandas_profiling import ProfileReport

from csvapi.errors import APIError
from csvapi.utils import get_db_info

import json


class CSVAPIProfileReport:

    def get_dataframe(self, db_info):
        dsn = 'file:{}?immutable=1'.format(db_info['db_path'])
        conn = sqlite3.connect(dsn, uri=True)
        sql = 'SELECT * FROM [{}]'.format(db_info['table_name'])
        df = pd.read_sql_query(sql, con=conn)
        return df

    async def get_minimal_profile(self, urlhash: str) -> dict:
        db_info = get_db_info(urlhash)
        p = Path(db_info['db_path'])
        if not p.exists():
            raise APIError('Database has probably been removed or does not exist yet.', status=404)

        try:
            df = self.get_dataframe(db_info)
            profile = ProfileReport(
                df, minimal=True,
                vars=dict(num={"low_categorical_threshold": 0}),
                plot=dict(histogram={"bins": 10}),
                # this disables the ThreadPoolExecutor in pandas-profiling
                # remove it or set it to 0 to use the number of CPUs a pool size
                pool_size=1,
                progress_bar=False,
            )
            profile_report = json.loads(profile.to_json())
            return profile_report
        except (sqlite3.OperationalError, sqlite3.IntegrityError) as e:
            raise APIError('Error selecting data', status=400, payload=dict(details=str(e)))
