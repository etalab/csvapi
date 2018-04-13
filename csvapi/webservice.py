import hashlib
import os
import tempfile

from pathlib import Path

import hupper
import json
import requests

from sanic import Sanic
from sanic import response
from sanic.exceptions import ServerError, abort
from sanic.request import RequestParameters

from .tableview import TableView
from .parser import parse

app = Sanic()
app.add_route(TableView.as_view(), '/api/<_hash>')


@app.route('/apify')
async def apify(request):
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
        parse(tmp.name, _hash, storage=app.config.DB_ROOT_DIR)
    finally:
        os.unlink(tmp.name)
    return response.json({
        'ok': True,
        'endpoint': '{}://{}/api/{}'.format(
            request.scheme, request.host, _hash
        ),
    }, dumps=json.dumps)
