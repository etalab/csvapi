import os
import traceback

from quart import Quart, jsonify
from quart_cors import cors
from werkzeug.exceptions import NotFound

from csvapi.errors import APIError
from csvapi.tableview import TableView
from csvapi.exportview import ExportView
from csvapi.uploadview import UploadView
from csvapi.parseview import ParseView
from csvapi.profileview import ProfileView
from csvapi.security import filter_referrers

app = Quart(__name__)
app = cors(app, allow_origin='*')

app.add_url_rule('/api/<urlhash>', view_func=TableView.as_view('table'))
app.add_url_rule('/profile/<urlhash>', view_func=ProfileView.as_view('profile'))
app.add_url_rule('/api/<urlhash>/export', view_func=ExportView.as_view('export'))
app.add_url_rule('/apify', view_func=ParseView.as_view('parse'))
app.add_url_rule('/upload', view_func=UploadView.as_view('upload'))
app.before_request(filter_referrers)


conffile = os.environ.get('CSVAPI_CONFIG_FILE') or '../config.py'
app.config.from_pyfile(conffile)


def handle_and_print_error(error):
    sentry_id = None
    if app.config.get('SENTRY_DSN'):
        import sentry_sdk
        with sentry_sdk.push_scope() as scope:
            sentry_sdk.init(
                app.config['SENTRY_DSN'],
                traces_sample_rate=1.0
            )
            scope.set_extra('debug', False)
            from sentry_sdk import capture_exception
            sentry_id = capture_exception(error)
    traceback.print_exc()
    return sentry_id


@app.errorhandler(NotFound)
def handle_not_found(error):
    response = jsonify({
        'ok': False,
        'error': 'Not found',
    })
    response.status_code = 404
    return response


@app.errorhandler(APIError)
def handle_api_error(error):
    error_id = handle_and_print_error(error)
    data = error.to_dict()
    app.logger.error(f"{data.get('error')}: {data.get('details', '')}")
    data['error_id'] = error_id
    response = jsonify(data)
    response.status_code = error.status
    return response


@app.errorhandler(Exception)
def handle_exceptions(error):
    """Serialize all errors to API"""
    error_id = handle_and_print_error(error)
    response = jsonify(error=str(error), error_id=error_id, ok=False)
    return response, 500
