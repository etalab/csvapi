import json
import hashlib
import os
import tempfile
import logging

from pathlib import Path

import requests

from sanic import response
from sanic.exceptions import abort
from sanic.views import HTTPMethodView

from csvapi.parser import parse
from csvapi.utils import get_db_info

log = logging.getLogger('__name__')


class ParseView(HTTPMethodView):

    async def options(self, request, *args, **kwargs):
        r = response.text('ok')
        r.headers['Access-Control-Allow-Origin'] = '*'
        return r

    def already_exists(self, storage, _hash):
        return Path(get_db_info(storage, _hash)['db_path']).exists()

    async def get(self, request):
        url = request.args.get('url')
        if not url:
            abort(400, 'Missing "url" query string variable.')
        _hash = hashlib.md5(url.encode('utf-8')).hexdigest()
        if not self.already_exists(request.app.config.DB_ROOT_DIR, _hash):
            tmp = tempfile.NamedTemporaryFile(delete=False)
            r = requests.get(url, stream=True)
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    tmp.write(chunk)
            tmp.close()
            try:
                parse(tmp.name, _hash, storage=request.app.config.DB_ROOT_DIR)
            finally:
                os.unlink(tmp.name)
        else:
            log.debug('{}.db already exists, skipping parse.'.format(_hash))
        return response.json({
            'ok': True,
            'endpoint': '{}://{}/api/{}'.format(
                request.scheme, request.host, _hash
            ),
        }, dumps=json.dumps, headers={'Access-Control-Allow-Origin': '*'})
