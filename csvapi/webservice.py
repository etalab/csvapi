import os
import traceback

from quart import Quart, jsonify

from csvapi.crossdomain import add_cors_headers
from csvapi.errors import APIError
from csvapi.tableview import TableView
from csvapi.parseview import ParseView

app = Quart(__name__)
app.add_url_rule('/api/<urlhash>', view_func=TableView.as_view('table'))
app.add_url_rule('/apify', view_func=ParseView.as_view('parse'))
app.after_request(add_cors_headers)

conffile = os.environ.get('CSVAPI_CONFIG_FILE') or '../config.py'
app.config.from_pyfile(conffile)

if app.config.get('SENTRY_DSN'):
    from raven import Client
    app.extensions['sentry'] = Client(app.config['SENTRY_DSN'])


def handle_and_print_error():
    sentry_id = None
    if app.extensions.get('sentry'):
        sentry_id = app.extensions['sentry'].captureException()
    traceback.print_exc()
    return sentry_id


@app.errorhandler(APIError)
def handle_api_error(error):
    error_id = handle_and_print_error()
    app.logger.error(error.message)
    data = error.to_dict()
    data['error_id'] = error_id
    response = jsonify(data)
    response.status_code = error.status
    return response


@app.errorhandler(Exception)
def handle_exceptions(error):
    """Serialize all errors to API"""
    error_id = handle_and_print_error()
    response = jsonify(error=str(error), error_id=error_id, ok=False)
    return response, 500
