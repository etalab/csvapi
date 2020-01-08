import os
from tempfile import NamedTemporaryFile

from quart import request, current_app as app, jsonify
from quart.views import MethodView

from csvapi.utils import get_hash_bytes
from csvapi.parser import parse


class UploadView(MethodView):

    async def options(self):
        pass

    async def post(self):
        try:
            tmpfile = NamedTemporaryFile(delete=False)
            # useless async?
            async for data in request.body:
                tmpfile.write(data)
            tmpfile.close()
            with open(tmpfile.name, 'rb') as _tmpfile:
                _hash = get_hash_bytes(_tmpfile.read())
            storage = app.config['DB_ROOT_DIR']
            sniff_limit = app.config.get('CSV_SNIFF_LIMIT')
            parse(tmpfile.name, _hash, storage, sniff_limit=sniff_limit)
        finally:
            os.unlink(tmpfile.name)

        scheme = 'https' if app.config.get('FORCE_SSL') else request.scheme
        return jsonify({
            'ok': True,
            'endpoint': f"{scheme}://{request.host}/api/{_hash}"
        })
