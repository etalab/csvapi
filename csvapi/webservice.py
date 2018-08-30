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

app.config.from_pyfile('../config.py')


@app.errorhandler(APIError)
def handle_api_error(error):
    app.logger.error(error.message)
    traceback.print_exc()
    response = jsonify(error.to_dict())
    response.status_code = error.status
    return response
