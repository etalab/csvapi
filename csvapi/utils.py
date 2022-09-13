import hashlib

from pathlib import Path

from quart import current_app as app

import sqlite3
from datetime import datetime
import pandas as pd

executor = None


def get_db_info(urlhash, storage=None):
    if app:
        # app.config not thread safe, sometimes we need to pass storage directly
        db_storage = storage or app.config['DB_ROOT_DIR']
        profile_storage = app.config['PROFILES_ROOT_DIR']

    db_path = f"{db_storage}/{urlhash}.db"
    profile_path = f"{profile_storage}/{urlhash}.html"
    return {
        'dsn': f"sqlite:///{db_path}",
        'db_name': urlhash,
        'table_name': urlhash,
        'db_path': db_path,
        'profile_path': profile_path,
    }


def get_hash(to_hash):
    return get_hash_bytes(to_hash.encode('utf-8'))


def get_hash_bytes(to_hash):
    return hashlib.md5(to_hash).hexdigest()


async def already_exists(urlhash, analysis=None):
    '''
    Check if db exist. If analysis is requested, we check if general_infos table exist.
    If not, we bypass cache and do a new download of file to analyse it with pp and csv-detective.
    '''
    cache_enabled = app.config.get('CSV_CACHE_ENABLED')
    if not cache_enabled:
        return False

    db_exist = Path(get_db_info(urlhash)['db_path']).exists()

    if not analysis or analysis != 'yes':
        return db_exist
    else:
        conn = create_connection(get_db_info(urlhash)['db_path'])
        cur = conn.cursor()
        sql = 'SELECT count(*) FROM sqlite_master WHERE type=\'table\' AND name=\'general_infos\''
        cur.execute(sql)
        rows = cur.fetchall()
        if rows[0][0] != 0:
            return True
        else:
            return False


def create_connection(db_file):
    conn = None
    conn = sqlite3.connect(db_file)
    return conn


def keys_exists(element, *keys):
    '''
    Check if *keys (nested) exists in `element` (dict).
    '''
    if not isinstance(element, dict):
        raise AttributeError('keys_exists() expects dict as first argument.')
    if len(keys) == 0:
        raise AttributeError('keys_exists() expects at least two arguments, one given.')
    _element = element
    for key in keys:
        try:
            _element = _element[key]
        except KeyError:
            return False
    return True


def check_csv_detective_report_structure(report):
    if (report is not None) and \
            (keys_exists(report, "columns")) and \
            (keys_exists(report, "encoding")) and \
            (keys_exists(report, "separator")) and \
            (keys_exists(report, "header_row_idx")):

        for item in report['columns']:
            if (not keys_exists(report, "columns", item, "python_type")) | \
                    (not keys_exists(report, "columns", item, "format")):
                return False
        return True
    else:
        return False


def check_profile_report_structure(report):
    if (report is not None) and \
            (keys_exists(report, "table", "n")) and \
            (keys_exists(report, "table", "n_var")) and \
            (keys_exists(report, "table", "n_cells_missing")) and \
            (keys_exists(report, "table", "n_vars_with_missing")) and \
            (keys_exists(report, "table", "n_vars_all_missing")) and \
            (keys_exists(report, "table", "n_cells_missing")) and \
            (keys_exists(report, "variables")):

        for item in report['variables']:
            if (not keys_exists(report, "variables", item, "n_distinct")) | \
                    (not keys_exists(report, "variables", item, "is_unique")) | \
                    (not keys_exists(report, "variables", item, "n_unique")) | \
                    (not keys_exists(report, "variables", item, "type")) | \
                    (not keys_exists(report, "variables", item, "n_missing")) | \
                    (not keys_exists(report, "variables", item, "count")):
                return False
        return True
    else:
        return False


def df_to_sql(obj, conn, name):
    df = pd.DataFrame(obj)
    if df.shape[0] > 0:
        df.to_sql(name, con=conn, if_exists='replace', index=False)


def enrich_db_with_metadata(urlhash, csv_detective_report, profile_report, dataset_id, key):
    # Save to sql
    conn = create_connection(app.config['DB_ROOT_DIR'] + '/' + urlhash + '.db')

    general_infos = [
        {
            'encoding': csv_detective_report['encoding'],
            'separator': csv_detective_report['separator'],
            'header_row_idx': csv_detective_report['header_row_idx'],
            'total_lines': profile_report['table']['n'],
            'nb_columns': profile_report['table']['n_var'],
            'nb_cells_missing': profile_report['table']['n_cells_missing'],
            'nb_vars_with_missing': profile_report['table']['n_vars_with_missing'],
            'nb_vars_all_missing': profile_report['table']['n_vars_all_missing'],
            'date_last_check': datetime.today().strftime('%Y-%m-%d'),
            'dataset_id': dataset_id,
            'resource_id': key
        }
    ]
    df = pd.DataFrame(general_infos)
    df.to_sql('general_infos', con=conn, if_exists='replace', index=False)

    columns_infos = []
    categorical_infos = []
    top_infos = []
    numeric_infos = []
    numeric_plot_infos = []
    for col in profile_report['variables']:
        column_info = {}
        column_info['name'] = col
        column_info['format'] = csv_detective_report['columns'][col]['format']
        column_info['nb_distinct'] = profile_report['variables'][col]['n_distinct']
        column_info['is_unique'] = profile_report['variables'][col]['is_unique']
        column_info['nb_unique'] = profile_report['variables'][col]['n_unique']
        column_info['type'] = profile_report['variables'][col]['type']
        column_info['nb_missing'] = profile_report['variables'][col]['n_missing']
        column_info['count'] = profile_report['variables'][col]['count']
        columns_infos.append(column_info)

        if csv_detective_report['columns'][col]['format'] in \
                ['siren', 'siret', 'code_postal', 'code_commune_insee', 'code_departement', 'code_region', 'tel_fr']:
            column_info['type'] = 'Categorical'

        if (column_info['type'] == 'Categorical') & \
                (len(profile_report['variables'][col]['value_counts_without_nan']) < 10):
            for cat in profile_report['variables'][col]['value_counts_without_nan']:
                categorical_info = {}
                categorical_info['column'] = col
                categorical_info['value'] = cat
                categorical_info['nb'] = profile_report['variables'][col]['value_counts_without_nan'][cat]
                categorical_infos.append(categorical_info)

        if column_info['type'] == 'Numeric':
            numeric_info = {}
            numeric_info['column'] = col
            numeric_info['mean'] = profile_report['variables'][col]['mean']
            numeric_info['std'] = profile_report['variables'][col]['std']
            numeric_info['min'] = profile_report['variables'][col]['min']
            numeric_info['max'] = profile_report['variables'][col]['max']
            numeric_infos.append(numeric_info)
            for i in range(len(profile_report['variables'][col]['histogram']['bin_edges'])):
                numeric_plot_info = {}
                numeric_plot_info['column'] = col
                numeric_plot_info['value'] = profile_report['variables'][col]['histogram']['bin_edges'][i]
                numeric_plot_info['type'] = 'bin_edges'
                numeric_plot_infos.append(numeric_plot_info)

            for i in range(len(profile_report['variables'][col]['histogram']['counts'])):
                numeric_plot_info = {}
                numeric_plot_info['column'] = col
                numeric_plot_info['value'] = profile_report['variables'][col]['histogram']['counts'][i]
                numeric_plot_info['type'] = 'counts'
                numeric_plot_infos.append(numeric_plot_info)

        cpt = 0
        for top in profile_report['variables'][col]['value_counts_without_nan']:
            if (cpt < 10):
                top_info = {}
                top_info['column'] = col
                top_info['value'] = top
                top_info['nb'] = profile_report['variables'][col]['value_counts_without_nan'][top]
                top_infos.append(top_info)
                cpt = cpt + 1

    df_to_sql(columns_infos, conn, 'columns_infos')
    df_to_sql(categorical_infos, conn, 'categorical_infos')
    df_to_sql(top_infos, conn, 'top_infos')
    df_to_sql(numeric_infos, conn, 'numeric_infos')
    df_to_sql(numeric_plot_infos, conn, 'numeric_plot_infos')

    conn.commit()
