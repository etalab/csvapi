from sanic import Sanic


from csvapi.tableview import TableView
from csvapi.parseview import ParseView

app = Sanic()
app.add_route(TableView.as_view(), '/api/<_hash>')
app.add_route(ParseView.as_view(), '/apify')
