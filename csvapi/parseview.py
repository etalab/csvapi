import hashlib
import os
import tempfile

import json
import requests

from sanic import response
from sanic.exceptions import abort
from sanic.views import HTTPMethodView

from csvapi.parser import parse


class ParseView(HTTPMethodView):

    async def options(self, request, *args, **kwargs):
        r = response.text('ok')
        r.headers['Access-Control-Allow-Origin'] = '*'
        return r

    async def get(self, request):
        url = request.args.get('url')
        if not url:
            abort(400, 'Missing "url" query string variable.')
        tmp = tempfile.NamedTemporaryFile(delete=False)
        r = requests.get(url, stream=True)
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                tmp.write(chunk)
        tmp.close()
        _hash = hashlib.md5(url.encode('utf-8')).hexdigest()
        try:
            await parse(tmp.name, _hash, storage=request.app.config.DB_ROOT_DIR)
        finally:
            os.unlink(tmp.name)
        return response.json({
            'ok': True,
            'endpoint': '{}://{}/api/{}'.format(
                request.scheme, request.host, _hash
            ),
        }, dumps=json.dumps, headers={'Access-Control-Allow-Origin': '*'})
