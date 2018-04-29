import asyncio
import hashlib
import logging
import os
import tempfile

from pathlib import Path

import requests
import validators

from quart import request, jsonify, current_app
from quart.views import MethodView

from csvapi.parser import parse
from csvapi.utils import get_db_info, api_error, get_executor

log = logging.getLogger('__name__')


class ParseView(MethodView):

    async def options(self):
        pass

    def already_exists(self, _hash):
        cache_enabled = current_app.config.get('CSV_CACHE_ENABLED')
        if not cache_enabled:
            return False
        storage = current_app.config['DB_ROOT_DIR']
        return Path(get_db_info(storage, _hash)['db_path']).exists()

    async def get(self):
        url = request.args.get('url')
        if not url:
            return api_error('Missing url query string variable.', 400)
        if not validators.url(url):
            return api_error('Malformed url parameter.', 400)
        _hash = hashlib.md5(url.encode('utf-8')).hexdigest()

        def do_parse_in_thread(storage):
            tmp = tempfile.NamedTemporaryFile(delete=False)
            r = requests.get(url, stream=True)
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    tmp.write(chunk)
            tmp.close()
            try:
                parse(tmp.name, _hash, storage=storage)
            except Exception as e:
                return api_error('Error parsing CSV', details=str(e))
            finally:
                os.unlink(tmp.name)

        if not self.already_exists(_hash):
            await asyncio.get_event_loop().run_in_executor(
                get_executor(), do_parse_in_thread, current_app.config['DB_ROOT_DIR']
            )
        else:
            log.debug('{}.db already exists, skipping parse.'.format(_hash))
        return jsonify({
            'ok': True,
            'endpoint': '{}://{}/api/{}'.format(
                request.scheme, request.host, _hash
            ),
        })
