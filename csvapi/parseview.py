import asyncio
import os
import tempfile

from pathlib import Path

import requests
import validators

from quart import request, jsonify, current_app as app
from quart.views import MethodView

from csvapi.errors import APIError
from csvapi.parser import parse
from csvapi.utils import get_db_info, get_executor, get_hash


class ParseView(MethodView):

    async def options(self):
        pass

    def already_exists(self, _hash):
        cache_enabled = app.config.get('CSV_CACHE_ENABLED')
        if not cache_enabled:
            return False
        storage = app.config['DB_ROOT_DIR']
        return Path(get_db_info(storage, _hash)['db_path']).exists()

    async def get(self):
        app.logger.debug('* Starting ParseView.get')
        url = request.args.get('url')
        if not url:
            raise APIError('Missing url query string variable.', status=400)
        if not validators.url(url):
            raise APIError('Malformed url parameter.', status=400)
        _hash = get_hash(url)

        def do_parse_in_thread(storage, logger):
            logger.debug('* do_parse_in_thread %s (%s)', _hash, url)
            tmp = tempfile.NamedTemporaryFile(delete=False)
            r = requests.get(url, stream=True)
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    tmp.write(chunk)
            tmp.close()
            logger.debug('* Downloaded %s', _hash)
            try:
                logger.debug('* Parsing %s...', _hash)
                parse(tmp.name, _hash, storage=storage)
                logger.debug('* Parsed %s', _hash)
            except Exception as e:
                raise APIError('Error parsing CSV', payload=dict(details=str(e)))
            finally:
                os.unlink(tmp.name)

        if not self.already_exists(_hash):
            await asyncio.get_event_loop().run_in_executor(
                get_executor(), do_parse_in_thread, app.config['DB_ROOT_DIR'], app.logger
            )
        else:
            app.logger.debug('{}.db already exists, skipping parse.'.format(_hash))
        return jsonify({
            'ok': True,
            'endpoint': '{}://{}/api/{}'.format(
                request.scheme, request.host, _hash
            ),
        })
