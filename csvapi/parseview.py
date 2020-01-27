import os
import tempfile
import time

import xxhash
import aiohttp
import validators

from quart import request, jsonify, current_app as app
from quart.views import MethodView

from csvapi.errors import APIError
from csvapi.parser import parse
from csvapi.utils import already_exists, get_hash

X = xxhash.xxh64()

class ParseView(MethodView):

    @staticmethod
    async def do_parse(url, encoding, storage, logger, sniff_limit, max_file_size):
        logger.debug('* do_parse (%s)', url)
        tmp = tempfile.NamedTemporaryFile(delete=False)
        chunk_count = 0
        chunk_size = 1024
        start_dl = time.time()
        try:
            # TODO: Is it possible to know any change in the hash of a file without downloading it to check it?
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    while True:
                        chunk = await resp.content.read(chunk_size)
                        if chunk_count * chunk_size > max_file_size:
                            tmp.close()
                            raise Exception('File too big (max size is %s bytes)' % max_file_size)
                        if not chunk:
                            break
                        X.update(chunk)
                        tmp.write(chunk)
                        chunk_count += 1
            tmp.close()
            filehash = X.hexdigest()
            print(filehash)
            logger.debug('* Downloaded %s', filehash)
            end_dl = time.time()
            print(f"--------------------------------> download time: {end_dl - start_dl}<------------------------------------")
            if not already_exists(filehash):
                try:
                    start_parse = time.time()
                    logger.debug('* Parsing %s...', filehash)
                    parse(tmp.name, filehash, storage, encoding=encoding, sniff_limit=sniff_limit)
                    logger.debug('* Parsed %s', filehash)
                    end_parse = time.time()
                    print(f"--------------------------------> parse time: {end_parse - start_parse}<------------------------------------")
                except Exception as e:
                    raise APIError('Error parsing CSV: %s' % e)
            else:
                logger.info(f"{filehash}.db already exists, skipping parse.")
            return filehash
        finally:
            logger.debug('Removing tmp file: %s', tmp.name)
            os.unlink(tmp.name)

    async def get(self):
        start = time.time()
        app.logger.debug('* Starting ParseView.get')
        url = request.args.get('url')
        encoding = request.args.get('encoding')
        if not url:
            raise APIError('Missing url query string variable.', status=400)
        if not validators.url(url):
            raise APIError('Malformed url parameter.', status=400)
        
        storage = app.config['DB_ROOT_DIR']
        filehash = await self.do_parse(url=url,
                            encoding=encoding,
                            storage=storage,
                            logger=app.logger,
                            sniff_limit=app.config.get('CSV_SNIFF_LIMIT'),
                            max_file_size=app.config.get('MAX_FILE_SIZE')
                            )

        # if not already_exists(urlhash):
        #     try:
        #         storage = app.config['DB_ROOT_DIR']
        #         await self.do_parse(url=url,
        #                             urlhash=urlhash,
        #                             encoding=encoding,
        #                             storage=storage,
        #                             logger=app.logger,
        #                             sniff_limit=app.config.get('CSV_SNIFF_LIMIT'),
        #                             max_file_size=app.config.get('MAX_FILE_SIZE')
        #                             )
        #     except Exception as e:
        #         raise APIError('Error parsing CSV: %s' % e)
        # else:
        #     app.logger.info(f"{urlhash}.db already exists, skipping parse.")
        scheme = 'https' if app.config.get('FORCE_SSL') else request.scheme
        end = time.time()
        timer = end - start
        print(f"--------------------------------> total execution time: {timer}<------------------------------------")
        return jsonify({
            'ok': True,
            'endpoint': f"{scheme}://{request.host}/api/{filehash}"
        })
