import logging
import asyncio
from csvapi.parseview import ParseView
from csvapi.profileview import ProfileView
from csvapi.utils import enrich_db_with_metadata
from csvapi.setup_logger import logger
import os
from csvapi.utils import get_hash, check_message_structure, check_csv_detective_report_structure, check_profile_report_structure

from config import DB_ROOT_DIR

MINIO_URL = os.environ.get("MINIO_URL", "http://localhost:9000")
MINIO_USER = os.environ.get("MINIO_USER", "minio")
MINIO_PASSWORD = os.environ.get("MINIO_PASSWORD", "password")


import boto3
from botocore.client import Config, ClientError
import json

def run_process_message(key: str, data: dict, topic: str) -> None:
    asyncio.get_event_loop().run_until_complete(process_message(key, data, topic))

async def process_message(key: str, message: dict, topic: str) -> None:
    if(message['service'] == "csvdetective"):
        if(check_message_structure(message)):
            #try:
                url = 'https://www.data.gouv.fr/fr/datasets/r/{}'.format(key)
                urlhash = get_hash(url) 
                logger.info(urlhash)   

                # Connect to minio
                s3_client = boto3.client(
                    "s3",
                    endpoint_url=MINIO_URL,
                    aws_access_key_id=MINIO_USER,
                    aws_secret_access_key=MINIO_PASSWORD,
                    config=Config(signature_version="s3v4"),
                )
                
                try:
                    s3_client.head_bucket(Bucket=message['value']['location']['bucket'])
                except ClientError as e:
                    logger.error(e)
                    logger.error(
                        "Bucket {} does not exist or credentials are invalid".format(
                            message['value']['location']['bucket']
                        )
                    )
                    return
                
                # Load csv-detective report
                try:
                    response = s3_client.get_object(Bucket = message['value']['location']['bucket'], Key = message['value']['location']['key'])
                    content = response['Body']
                    csv_detective_report = json.loads(content.read())
                except ClientError as e:
                    logger.error(e)
                    logger.error(
                        "Report does not exist in bucket or content is not valid json"
                    )
                    return 
                
                if not check_csv_detective_report_structure(csv_detective_report):
                    logger.error(
                        "csvdetective report malformed"
                    )
                    return
                
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

                if not check_profile_report_structure(profile_report):
                    logger.error(
                        "pandas profiling report malformed"
                    )
                    return

                enrich_db_with_metadata(
                    urlhash,
                    csv_detective_report,
                    profile_report,
                    message['meta']['dataset_id'],
                    key
                )

                logger.info('Enrichment done!')


            #except:
            #    logger.info('Error with message', message)
        else:
            logger.error('Problem with structure message')
    else:
        logger.info('Message received from {} service - do not process'.format(message['service']))
        
