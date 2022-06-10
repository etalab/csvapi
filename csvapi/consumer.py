import logging
import asyncio
from csvapi.parseview import ParseView
from csvapi.profileview import ProfileView
from csvapi.setup_logger import logger
import requests

from config import MINIO_URL, MINIO_USER, MINIO_PASSWORD, DB_ROOT_DIR

from csvapi.utils import get_hash, create_connection

import boto3
from botocore.client import Config, ClientError
import json
from datetime import datetime
import pandas as pd

def run_process_message(key: str, data: dict, topic: str) -> None:
    asyncio.get_event_loop().run_until_complete(process_message(key, data, topic))

async def process_message(key: str, message: dict, topic: str) -> None:
    # Get url
    # Should think if we keep that
    #r = requests.get('https://www.data.gouv.fr/api/1/datasets/{}/resources/{}'.format(message['meta']['dataset_id'], key))
    #url = r.json()['url']
    url = 'https://www.data.gouv.fr/fr/datasets/r/{}'.format(key)
    logger.info(url)
    urlhash = get_hash(url)    

    # Connect to minio
    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_URL,
        aws_access_key_id=MINIO_USER,
        aws_secret_access_key=MINIO_PASSWORD,
        config=Config(signature_version="s3v4"),
    )
    
    try:
        s3_client.head_bucket(Bucket=message['value']['bucket'])
        logger.info('ok bucket')
    except ClientError as e:
        logger.error(e)
        logger.error(
            "Bucket {} does not exist or credentials are invalid".format(
                message['value']['bucket']
            )
        )
        return
    
    # Load csv-detective report
    response = s3_client.get_object(Bucket = message['value']['bucket'], Key = message['value']['report'])
    content = response['Body']
    csv_detective_report = json.loads(content.read())

    # Parse file and store it to sqlite
    parseViewInstance = ParseView()
    await parseViewInstance.parse_from_consumer(
        parseViewInstance,
        url=url,
        urlhash=urlhash,
        csv_detective_report = csv_detective_report
    )

    # Profile file
    profileViewInstance = ProfileView()
    profile_report = await profileViewInstance.get_minimal_profile(
        profileViewInstance,
        urlhash=urlhash,
        csv_detective_report = csv_detective_report
    )

    # Save to sql
    conn = create_connection(DB_ROOT_DIR+'/'+urlhash+'.db')
    #c = conn.cursor()
    
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
            'date_last_check': datetime.today().strftime('%Y-%m-%d')
        }
    ]
    df = pd.DataFrame(general_infos)
    df.to_sql('general_infos',con=conn, if_exists='replace', index=False)

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
        logger.info(column_info)

        if((profile_report['variables'][col]['type'] == 'Categorical') & (len(profile_report['variables'][col]['value_counts_without_nan']) < 10)):
            for cat in profile_report['variables'][col]['value_counts_without_nan']:
                categorical_info = {}
                categorical_info['column'] = col
                categorical_info['value'] = cat
                categorical_info['nb'] = profile_report['variables'][col]['value_counts_without_nan'][cat]
                categorical_infos.append(categorical_info)

        if(profile_report['variables'][col]['type'] == 'Numeric'):
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
    
    df = pd.DataFrame(columns_infos)
    df.to_sql('column_infos',con=conn, if_exists='replace', index=False)
    df = pd.DataFrame(categorical_infos)
    df.to_sql('categorical_infos',con=conn, if_exists='replace', index=False)
    df = pd.DataFrame(top_infos)
    df.to_sql('top_infos',con=conn, if_exists='replace', index=False)
    df = pd.DataFrame(numeric_infos)
    df.to_sql('numeric_infos',con=conn, if_exists='replace', index=False)
    df = pd.DataFrame(numeric_plot_infos)
    df.to_sql('numeric_plot_infos',con=conn, if_exists='replace', index=False)
    conn.commit()

    print('ok')

    # Consolider detection de type pandas profiling
    # on dirait qu'il ne comprend pas le dtype à la lecture (notamment sur siren)

