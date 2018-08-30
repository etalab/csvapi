import os
from pathlib import Path

import pytest
import requests_mock

from csvapi.utils import get_hash
from csvapi.webservice import app as csvapi_app

MOCK_CSV_URL = 'http://domain.com/file.csv'
MOCK_CSV_HASH = get_hash(MOCK_CSV_URL)
DB_ROOT_DIR = './tests/dbs'


@pytest.fixture
def rmock():
    with requests_mock.mock() as m:
        yield m


@pytest.fixture
def app():
    csvapi_app.config.update({
        'DB_ROOT_DIR': DB_ROOT_DIR,
        'CSV_CACHE_ENABLED': False,
    })
    yield csvapi_app
    [db.unlink() for db in Path(DB_ROOT_DIR).glob('*.db')]


@pytest.fixture
def app_w_cache(app):
    app.config.update({
        'CSV_CACHE_ENABLED': True,
    })
    yield app
    [db.unlink() for db in Path(DB_ROOT_DIR).glob('*.db')]


@pytest.fixture
def client(app):
    yield app.test_client()


@pytest.fixture
def client_w_cache(app_w_cache):
    yield app_w_cache.test_client()


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


@pytest.fixture
@pytest.mark.asyncio
async def uploaded_csv(rmock, csv, client):
    content = csv.replace('<sep>', ';').encode('utf-8')
    rmock.get(MOCK_CSV_URL, content=content)
    await client.get('/apify?url={}'.format(MOCK_CSV_URL))


@pytest.mark.asyncio
async def test_apify_no_url(rmock, csv, client):
    res = await client.get('/apify')
    assert res.status_code == 400


@pytest.mark.asyncio
async def test_apify_wrong_url(rmock, csv, client):
    res = await client.get('/apify?url=notanurl')
    assert res.status_code == 400


@pytest.mark.asyncio
async def test_apify(rmock, csv, client):
    rmock.get(MOCK_CSV_URL, content=csv.encode('utf-8'))
    res = await client.get('/apify?url={}'.format(MOCK_CSV_URL))
    assert res.status_code == 200
    jsonres = await res.json
    assert jsonres['ok']
    assert 'endpoint' in jsonres
    assert '/api/{}'.format(MOCK_CSV_HASH) in jsonres['endpoint']
    db_path = Path(DB_ROOT_DIR) / '{}.db'.format(MOCK_CSV_HASH)
    assert db_path.exists()


@pytest.mark.asyncio
async def test_apify_w_cache(rmock, csv, client_w_cache):
    rmock.get(MOCK_CSV_URL, content=csv.encode('utf-8'))
    res = await client_w_cache.get('/apify?url={}'.format(MOCK_CSV_URL))
    assert res.status_code == 200
    jsonres = await res.json
    assert jsonres['ok']
    assert 'endpoint' in jsonres
    assert '/api/{}'.format(MOCK_CSV_HASH) in jsonres['endpoint']
    db_path = Path(DB_ROOT_DIR) / '{}.db'.format(MOCK_CSV_HASH)
    assert db_path.exists()


@pytest.mark.asyncio
async def test_apify_col_mismatch(rmock, csv_col_mismatch, client):
    rmock.get(MOCK_CSV_URL, content=csv_col_mismatch.replace('<sep>', ';').encode('utf-8'))
    res = await client.get('/apify?url={}'.format(MOCK_CSV_URL))
    assert res.status_code == 200
    jsonres = await res.json
    assert jsonres['ok']


@pytest.mark.asyncio
@pytest.mark.parametrize('separator', [';', ',', '\t'])
@pytest.mark.parametrize('encoding', ['utf-8', 'iso-8859-15', 'iso-8859-1'])
async def test_api(client, rmock, csv, separator, encoding):
    content = csv.replace('<sep>', separator).encode(encoding)
    rmock.get(MOCK_CSV_URL, content=content)
    await client.get('/apify?url={}'.format(MOCK_CSV_URL))
    res = await client.get('/api/{}'.format(MOCK_CSV_HASH))
    assert res.status_code == 200
    jsonres = await res.json
    assert jsonres['columns'] == ['rowid', 'col a', 'col b', 'col c']
    assert jsonres['rows'] == [
        [1, 'data à1', 'data b1', 'z'],
        [2, 'data ª2', 'data b2', 'a'],
    ]


@pytest.mark.asyncio
async def test_api_limit(client, rmock, uploaded_csv):
    res = await client.get('/api/{}?_size=1'.format(MOCK_CSV_HASH))
    assert res.status_code == 200
    jsonres = await res.json
    assert len(jsonres['rows']) == 1
    assert jsonres['rows'] == [
        [1, 'data à1', 'data b1', 'z'],
    ]


@pytest.mark.asyncio
async def test_api_limit_offset(client, rmock, uploaded_csv):
    res = await client.get('/api/{}?_size=1&_offset=1'.format(MOCK_CSV_HASH))
    assert res.status_code == 200
    jsonres = await res.json
    assert len(jsonres['rows']) == 1
    assert jsonres['rows'] == [
        [2, 'data ª2', 'data b2', 'a'],
    ]


@pytest.mark.asyncio
async def test_api_wrong_limit(client, rmock, uploaded_csv):
    res = await client.get('/api/{}?_size=toto'.format(MOCK_CSV_HASH))
    assert res.status_code == 400


@pytest.mark.asyncio
async def test_api_wrong_shape(client, rmock, uploaded_csv):
    res = await client.get('/api/{}?_shape=toto'.format(MOCK_CSV_HASH))
    assert res.status_code == 400


@pytest.mark.asyncio
async def test_api_objects_shape(client, rmock, uploaded_csv):
    res = await client.get('/api/{}?_shape=objects'.format(MOCK_CSV_HASH))
    assert res.status_code == 200
    jsonres = await res.json
    assert jsonres['rows'] == [{
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


@pytest.mark.asyncio
async def test_api_objects_norowid(client, rmock, uploaded_csv):
    res = await client.get('/api/{}?_shape=objects&_rowid=hide'.format(MOCK_CSV_HASH))
    assert res.status_code == 200
    jsonres = await res.json
    assert jsonres['rows'] == [{
            'col a': 'data à1',
            'col b': 'data b1',
            'col c': 'z',
        }, {
            'col a': 'data ª2',
            'col b': 'data b2',
            'col c': 'a',
    }]


@pytest.mark.asyncio
async def test_api_sort(client, rmock, uploaded_csv):
    res = await client.get('/api/{}?_sort=col c'.format(MOCK_CSV_HASH))
    assert res.status_code == 200
    jsonres = await res.json
    assert jsonres['rows'] == [
        [2, 'data ª2', 'data b2', 'a'],
        [1, 'data à1', 'data b1', 'z'],
    ]


@pytest.mark.asyncio
async def test_api_sort_desc(client, rmock, uploaded_csv):
    res = await client.get('/api/{}?_sort_desc=col b'.format(MOCK_CSV_HASH))
    assert res.status_code == 200
    jsonres = await res.json
    assert jsonres['rows'] == [
        [2, 'data ª2', 'data b2', 'a'],
        [1, 'data à1', 'data b1', 'z'],
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize('extension', ['xls', 'xlsx'])
async def test_api_excel(client, rmock, extension):
    here = os.path.dirname(os.path.abspath(__file__))
    content = open('{}/samples/test.{}'.format(here, extension), 'rb')
    mock_url = MOCK_CSV_URL.replace('.csv', extension)
    mock_hash = get_hash(mock_url)
    rmock.get(mock_url, content=content.read())
    content.close()
    await client.get('/apify?url={}'.format(mock_url))
    res = await client.get('/api/{}'.format(mock_hash))
    assert res.status_code == 200
    jsonres = await res.json
    assert jsonres['columns'] == ['rowid', 'col a', 'col b', 'col c']
    assert jsonres['rows'] == [
        [1, 'a1', 'b1', 'z'],
        [2, 'a2', 'b2', 'a'],
    ]
