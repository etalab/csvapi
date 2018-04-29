import os
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
    csvapi_app.config.CSV_CACHE_ENABLED = False
    yield csvapi_app
    [db.unlink() for db in Path(DB_ROOT_DIR).glob('*.db')]


@pytest.fixture
def client(app):
    yield app.test_client


@pytest.fixture
def csv():
    return '''col a<sep>col b<sep>col c
data à1<sep>data b1<sep>z
data ª2<sep>data b2<sep>a
'''


@pytest.fixture
def csv_col_mismatch():
    return '''col a<sep>col b
data à1<sep>data b1<sep>2
data ª2<sep>data b2<sep>4<sep>
'''


def test_apify_no_url(rmock, csv, client):
    _, res = client.get('/apify')
    assert res.status == 400


def test_apify_wrong_url(rmock, csv, client):
    _, res = client.get('/apify?url=notanurl')
    assert res.status == 400


def test_apify(rmock, csv, client):
    rmock.get(MOCK_CSV_URL, content=csv.encode('utf-8'))
    req, res = client.get('/apify?url={}'.format(MOCK_CSV_URL))
    assert res.status == 200
    assert res.json['ok']
    assert 'endpoint' in res.json
    assert '/api/{}'.format(MOCK_CSV_HASH) in res.json['endpoint']
    db_path = Path(DB_ROOT_DIR) / '{}.db'.format(MOCK_CSV_HASH)
    assert db_path.exists()


def test_apify_col_mismatch(rmock, csv_col_mismatch, client):
    rmock.get(MOCK_CSV_URL, content=csv_col_mismatch.replace('<sep>', ';').encode('utf-8'))
    req, res = client.get('/apify?url={}'.format(MOCK_CSV_URL))
    assert res.status == 200
    assert res.json['ok']


@pytest.mark.parametrize('separator', [';', ',', '\t'])
@pytest.mark.parametrize('encoding', ['utf-8', 'iso-8859-15', 'iso-8859-1'])
def test_api(client, rmock, csv, separator, encoding):
    content = csv.replace('<sep>', separator).encode(encoding)
    rmock.get(MOCK_CSV_URL, content=content)
    client.get('/apify?url={}'.format(MOCK_CSV_URL))
    _, res = client.get('/api/{}'.format(MOCK_CSV_HASH))
    assert res.status == 200
    assert res.json['columns'] == ['rowid', 'col a', 'col b', 'col c']
    assert res.json['rows'] == [
        [1, 'data à1', 'data b1', 'z'],
        [2, 'data ª2', 'data b2', 'a'],
    ]


def test_api_limit(client, rmock, csv):
    content = csv.replace('<sep>', ';').encode('utf-8')
    rmock.get(MOCK_CSV_URL, content=content)
    client.get('/apify?url={}'.format(MOCK_CSV_URL))
    _, res = client.get('/api/{}?_size=1'.format(MOCK_CSV_HASH))
    assert res.status == 200
    assert len(res.json['rows']) == 1
    assert res.json['rows'] == [
        [1, 'data à1', 'data b1', 'z'],
    ]


def test_api_limit_offset(client, rmock, csv):
    content = csv.replace('<sep>', ';').encode('utf-8')
    rmock.get(MOCK_CSV_URL, content=content)
    client.get('/apify?url={}'.format(MOCK_CSV_URL))
    _, res = client.get('/api/{}?_size=1&_offset=1'.format(MOCK_CSV_HASH))
    assert res.status == 200
    assert len(res.json['rows']) == 1
    assert res.json['rows'] == [
        [2, 'data ª2', 'data b2', 'a'],
    ]


def test_api_wrong_limit(client, rmock, csv):
    content = csv.replace('<sep>', ';').encode('utf-8')
    rmock.get(MOCK_CSV_URL, content=content)
    client.get('/apify?url={}'.format(MOCK_CSV_URL))
    _, res = client.get('/api/{}?_size=toto'.format(MOCK_CSV_HASH))
    assert res.status == 400


def test_api_wrong_shape(client, rmock, csv):
    content = csv.replace('<sep>', ';').encode('utf-8')
    rmock.get(MOCK_CSV_URL, content=content)
    client.get('/apify?url={}'.format(MOCK_CSV_URL))
    _, res = client.get('/api/{}?_shape=toto'.format(MOCK_CSV_HASH))
    assert res.status == 400


def test_api_objects_shape(client, rmock, csv):
    content = csv.replace('<sep>', ';').encode('utf-8')
    rmock.get(MOCK_CSV_URL, content=content)
    client.get('/apify?url={}'.format(MOCK_CSV_URL))
    _, res = client.get('/api/{}?_shape=objects'.format(MOCK_CSV_HASH))
    assert res.status == 200
    assert res.json['rows'] == [{
            'rowid': 1,
            'col a': 'data à1',
            'col b': 'data b1',
            'col c': 'z',
        }, {
            'rowid': 2,
            'col a': 'data ª2',
            'col b': 'data b2',
            'col c': 'a',
    }]


def test_api_objects_norowid(client, rmock, csv):
    content = csv.replace('<sep>', ';').encode('utf-8')
    rmock.get(MOCK_CSV_URL, content=content)
    client.get('/apify?url={}'.format(MOCK_CSV_URL))
    _, res = client.get('/api/{}?_shape=objects&_rowid=hide'.format(MOCK_CSV_HASH))
    assert res.status == 200
    assert res.json['rows'] == [{
            'col a': 'data à1',
            'col b': 'data b1',
            'col c': 'z',
        }, {
            'col a': 'data ª2',
            'col b': 'data b2',
            'col c': 'a',
    }]


def test_api_sort(client, rmock, csv):
    content = csv.replace('<sep>', ';').encode('utf-8')
    rmock.get(MOCK_CSV_URL, content=content)
    client.get('/apify?url={}'.format(MOCK_CSV_URL))
    _, res = client.get('/api/{}?_sort=col c'.format(MOCK_CSV_HASH))
    assert res.status == 200
    assert res.json['rows'] == [
        [2, 'data ª2', 'data b2', 'a'],
        [1, 'data à1', 'data b1', 'z'],
    ]


def test_api_sort_desc(client, rmock, csv):
    content = csv.replace('<sep>', ';').encode('utf-8')
    rmock.get(MOCK_CSV_URL, content=content)
    client.get('/apify?url={}'.format(MOCK_CSV_URL))
    _, res = client.get('/api/{}?_sort_desc=col b'.format(MOCK_CSV_HASH))
    assert res.status == 200
    assert res.json['rows'] == [
        [2, 'data ª2', 'data b2', 'a'],
        [1, 'data à1', 'data b1', 'z'],
    ]


@pytest.mark.parametrize('extension', ['xls', 'xlsx'])
def test_api_excel(client, rmock, csv, extension):
    here = os.path.dirname(os.path.abspath(__file__))
    content = open('{}/samples/test.{}'.format(here, extension), 'rb')
    rmock.get(MOCK_CSV_URL, content=content.read())
    content.close()
    client.get('/apify?url={}'.format(MOCK_CSV_URL))
    _, res = client.get('/api/{}'.format(MOCK_CSV_HASH))
    assert res.status == 200
    assert res.json['columns'] == ['rowid', 'col a', 'col b', 'col c']
    assert res.json['rows'] == [
        [1, 'a1', 'b1', 'z'],
        [2, 'a2', 'b2', 'a'],
    ]
