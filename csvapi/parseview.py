import asyncio
import hashlib
import json
import os
import tempfile

from pathlib import Path

import requests
import validators

from sanic import response
from sanic.views import HTTPMethodView
from sanic.log import logger as log

from csvapi.parser import parse
from csvapi.utils import get_db_info, api_error, api_error_from_e


class ParseView(HTTPMethodView):

    def already_exists(self, app, _hash):
        cache_enabled = app.config.get('CSV_CACHE_ENABLED')
        if not cache_enabled:
            return False
        storage = app.config.DB_ROOT_DIR
        return Path(get_db_info(storage, _hash)['db_path']).exists()

    async def get(self, request):
        url = request.args.get('url')
        encoding = request.args.get('encoding')
        if not url:
            return api_error('Missing url query string variable.', 400)
        if not validators.url(url):
            return api_error('Malformed url parameter.', 400)
        _hash = hashlib.md5(url.encode('utf-8')).hexdigest()

        def do_parse_in_thread():
            log.debug('Downloading file... %s' % url)
            tmp = tempfile.NamedTemporaryFile(delete=False)
            r = requests.get(url, stream=True)
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    tmp.write(chunk)
            tmp.close()
            try:
                log.debug('Launching parse...')
                parse(tmp.name, _hash, storage=request.app.config.DB_ROOT_DIR, encoding=encoding)
            finally:
                log.debug('Removing tmp file: %s', tmp.name)
                os.unlink(tmp.name)

        if not self.already_exists(request.app, _hash):
            try:
                await asyncio.get_event_loop().run_in_executor(
                    request.app.executor, do_parse_in_thread
                )
            except Exception as e:
                return api_error_from_e('Error parsing CSV', e)
        else:
            log.info('{}.db already exists, skipping parse.'.format(_hash))
        return response.json({
            'ok': True,
            'endpoint': '{}://{}/api/{}'.format(
                request.scheme, request.host, _hash
            ),
        }, dumps=json.dumps)
