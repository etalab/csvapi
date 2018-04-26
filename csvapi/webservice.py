from concurrent import futures

from sanic import Sanic
from sanic_cors import CORS

from csvapi.tableview import TableView
from csvapi.parseview import ParseView

app = Sanic()
CORS(app)
app.add_route(TableView.as_view(), '/api/<_hash>')
app.add_route(ParseView.as_view(), '/apify')


@app.listener('before_server_start')
async def setup_executor(app, loop):
    max_workers = app.config.get('MAX_WORKERS')
    app.executor = futures.ThreadPoolExecutor(max_workers=max_workers)
