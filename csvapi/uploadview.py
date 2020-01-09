import os
from tempfile import NamedTemporaryFile

from quart import request, current_app as app, jsonify
from quart.views import MethodView

from csvapi.errors import APIError
from csvapi.utils import get_hash_bytes, already_exists
from csvapi.parser import parse


class UploadView(MethodView):

    async def post(self):
        files = await request.files
        _file = files.get('file') or files.get('filepond')
        if not _file:
            raise APIError('Missing file.', status=400)
        content_hash = get_hash_bytes(_file.read())
        _file.seek(0)
        if not already_exists(content_hash):
            storage = app.config['DB_ROOT_DIR']
            sniff_limit = app.config.get('CSV_SNIFF_LIMIT')
            try:
                _tmpfile = NamedTemporaryFile(delete=False)
                _file.save(_tmpfile)
                _tmpfile.close()
                parse(_tmpfile.name, content_hash, storage, sniff_limit=sniff_limit)
            finally:
                os.unlink(_tmpfile.name)

        scheme = 'https' if app.config.get('FORCE_SSL') else request.scheme
        return jsonify({
            'ok': True,
            'endpoint': f"{scheme}://{request.host}/api/{content_hash}"
        })
