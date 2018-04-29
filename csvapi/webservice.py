from quart import Quart

from csvapi.tableview import TableView
from csvapi.parseview import ParseView

app = Quart(__name__)
app.add_url_rule('/api/<urlhash>', view_func=TableView.as_view('table'))
app.add_url_rule('/apify', view_func=ParseView.as_view('parse'))
