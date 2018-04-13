from pathlib import Path

import pytest
import requests_mock

from csvapi.webservice import app as csvapi_app

MOCK_CSV_URL = 'http://domain.com/file.csv'
MOCK_CSV_HASH = '7c3faa966a8ef777d0e3083f373be907'
DB_ROOT_DIR = './tests/dbs'


@pytest.fixture
def rmock():
    with requests_mock.mock() as m:
        yield m


@pytest.fixture
def app():
    csvapi_app.config.DB_ROOT_DIR = DB_ROOT_DIR
    yield csvapi_app
    [db.unlink() for db in Path(DB_ROOT_DIR).glob('*.db')]


@pytest.fixture
def client(app):
    yield app.test_client


@pytest.fixture
def csv():
    return '''col a<sep>col b
data à1<sep>data b1
data ª2<sep>data b2
'''


def test_apify(rmock, csv, client):
    rmock.get(MOCK_CSV_URL, content=csv.encode('utf-8'))
    _, res = client.get('/apify')
    assert res.status == 400
    req, res = client.get('/apify?url={}'.format(MOCK_CSV_URL))
    assert res.status == 200
    assert res.json['ok']
    assert 'endpoint' in res.json
    assert '/api/{}'.format(MOCK_CSV_HASH) in res.json['endpoint']
    db_path = Path(DB_ROOT_DIR) / '{}.db'.format(MOCK_CSV_HASH)
    assert db_path.exists()


@pytest.mark.parametrize('separator', [';', ',', '\t'])
@pytest.mark.parametrize('encoding', ['utf-8', 'iso-8859-15', 'iso-8859-1'])
def test_api(client, rmock, csv, separator, encoding):
    content = csv.replace('<sep>', separator).encode(encoding)
    rmock.get(MOCK_CSV_URL, content=content)
    client.get('/apify?url={}'.format(MOCK_CSV_URL))
    _, res = client.get('/api/{}'.format(MOCK_CSV_HASH))
    assert res.status == 200
    assert res.json['columns'] == ['rowid', 'col a', 'col b']
    assert res.json['rows'] == [
        [1, 'data à1', 'data b1'],
        [2, 'data ª2', 'data b2'],
    ]
