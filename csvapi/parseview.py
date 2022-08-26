import os
import tempfile

import aiohttp
import validators

from quart import request, jsonify, current_app as app
from quart.views import MethodView

from csvapi.errors import APIError
from csvapi.parser import parse
from csvapi.utils import already_exists, get_hash

from csvapi.setup_logger import logger
from csvapi.type_tester import convert_types
from config import DB_ROOT_DIR, CSV_SNIFF_LIMIT, MAX_FILE_SIZE


class ParseView(MethodView):

    @staticmethod
    async def parse_from_consumer(self, url: str, urlhash: str, csv_detective_report: dict) -> None:
        storage = DB_ROOT_DIR
        column_types = []
        for col in csv_detective_report['columns']:
            column_types.append(csv_detective_report['columns'][col]['python_type'])
        agate_types = convert_types(column_types)

        await self.do_parse(
            url=url,
            urlhash=urlhash,
            encoding=csv_detective_report['encoding'],
            storage=storage,
            logger=logger,
            sniff_limit=CSV_SNIFF_LIMIT,
            max_file_size=MAX_FILE_SIZE,
            agate_types=agate_types
        )

    @staticmethod
    async def do_parse(url, urlhash, encoding, storage, logger, sniff_limit, max_file_size, agate_types=None):
        logger.debug('* do_parse %s (%s)', urlhash, url)
        tmp = tempfile.NamedTemporaryFile(delete=False)
        chunk_count = 0
        chunk_size = 1024
        try:
            async with aiohttp.ClientSession(raise_for_status=True) as session:
                async with session.get(url) as resp:
                    while True:
                        chunk = await resp.content.read(chunk_size)
                        if chunk_count * chunk_size > max_file_size:
                            tmp.close()
                            raise Exception('File too big (max size is %s bytes)' % max_file_size)
                        if not chunk:
                            break
                        tmp.write(chunk)
                        chunk_count += 1
            tmp.close()
            logger.debug('* Downloaded %s', urlhash)
            logger.debug('* Parsing %s...', urlhash)
            parse(tmp.name, urlhash, storage, encoding=encoding, sniff_limit=sniff_limit, agate_types=agate_types)
            logger.debug('* Parsed %s', urlhash)
        finally:
            logger.debug('Removing tmp file: %s', tmp.name)
            os.unlink(tmp.name)

    async def get(self):
        app.logger.debug('* Starting ParseView.get')
        url = request.args.get('url')
        encoding = request.args.get('encoding')
        if not url:
            raise APIError('Missing url query string variable.', status=400)
        if not validators.url(url):
            raise APIError('Malformed url parameter.', status=400)
        urlhash = get_hash(url)

        if not already_exists(urlhash):
            try:
                storage = app.config['DB_ROOT_DIR']
                await self.do_parse(url=url,
                                    urlhash=urlhash,
                                    encoding=encoding,
                                    storage=storage,
                                    logger=app.logger,
                                    sniff_limit=app.config.get('CSV_SNIFF_LIMIT'),
                                    max_file_size=app.config.get('MAX_FILE_SIZE')
                                    )
            except Exception as e:
                raise APIError('Error parsing CSV: %s' % e)
        else:
            app.logger.info(f"{urlhash}.db already exists, skipping parse.")
        scheme = 'https' if app.config.get('FORCE_SSL') else request.scheme
        return jsonify({
            'ok': True,
            'endpoint': f"{scheme}://{request.host}/api/{urlhash}",
            'profile_endpoint': f"{scheme}://{request.host}/profile/{urlhash}",
        })
