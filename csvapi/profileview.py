from pathlib import Path

import pandas as pd
import sqlite3

from quart import send_from_directory
from quart.views import MethodView
from pandas_profiling import ProfileReport

from csvapi.errors import APIError
from csvapi.utils import get_db_info

from csvapi.type_tester import convert_python_types

from quart import current_app as app

import json


class ProfileView(MethodView):

    def get_dataframe(self, db_info, dtype=None):
        dsn = 'file:{}?immutable=1'.format(db_info['db_path'])
        conn = sqlite3.connect(dsn, uri=True)
        sql = 'SELECT * FROM [{}]'.format(db_info['table_name'])
        try:
            df = pd.read_sql_query(sql, con=conn, dtype=dtype)
        # TODO: check if correct exception type
        except ValueError:
            df = pd.read_sql_query(sql, con=conn)
            app.logger.debug('problem with python types')
        return df

    def make_profile(self, db_info):
        df = self.get_dataframe(db_info)

        if app.config['PANDAS_PROFILING_CONFIG_MIN']:
            profile = ProfileReport(df, config_file="profiling-minimal.yml")
        else:
            profile = ProfileReport(df)
        profile.to_file(db_info['profile_path'])
        return Path(db_info['profile_path'])

    async def get(self, urlhash):
        db_info = get_db_info(urlhash)
        p = Path(db_info['db_path'])
        if not p.exists():
            raise APIError('Database has probably been removed or does not exist yet.', status=404)

        path = Path(db_info['profile_path'])

        if not path.exists():
            try:
                path = self.make_profile(db_info)
            except (sqlite3.OperationalError, sqlite3.IntegrityError) as e:
                raise APIError('Error selecting data', status=400, payload=dict(details=str(e)))

        return await send_from_directory(path.parent, path.name)

    async def get_minimal_profile(self, url: str, urlhash: str, csv_detective_report: dict) -> None:
        db_info = get_db_info(urlhash)
        p = Path(db_info['db_path'])
        if not p.exists():
            raise APIError('Database has probably been removed or does not exist yet.', status=404)

        path = Path(db_info['profile_path'])

        if not path.exists():
            try:

                python_types = convert_python_types(csv_detective_report['columns'])
                df = self.get_dataframe(db_info, dtype=python_types)
                profile = ProfileReport(
                    df, minimal=True,
                    vars=dict(num={"low_categorical_threshold": 0}),
                    plot=dict(histogram={"bins": 10})
                )
                profile_report = json.loads(profile.to_json())
                return profile_report
            except (sqlite3.OperationalError, sqlite3.IntegrityError) as e:
                raise APIError('Error selecting data', status=400, payload=dict(details=str(e)))

        return await send_from_directory(path.parent, path.name)
