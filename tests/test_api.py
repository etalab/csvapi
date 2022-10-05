import os
import uuid
from pathlib import Path

import pytest
import pytest_asyncio
from aioresponses import aioresponses

from csvapi.utils import get_hash
from csvapi.webservice import app as csvapi_app

MOCK_CSV_URL = 'http://domain.com/file.csv'
MOCK_CSV_URL_FILTERS = 'http://domain.com/filters.csv'
MOCK_CSV_HASH_FILTERS = get_hash(MOCK_CSV_URL_FILTERS)
MOCK_CSV_HASH = get_hash(MOCK_CSV_URL)
DB_ROOT_DIR = './tests/dbs'


pytestmark = pytest.mark.asyncio


@pytest.fixture
def rmock():
    with aioresponses() as m:
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
def client(app):
    yield app.test_client()


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
def csv_hour():
    return '''id<sep>hour
a<sep>12:30
b<sep>9:15
c<sep>09:45
'''


@pytest.fixture
def csv_filters():
    """
    TODO: also test with unicode value in column name, but Quart
    test client currently fails
    """
    return '''id,hour,value,another column
first,12:30,1,value
second,9:15,2,value
third,09:45,3,value
'''


@pytest.fixture
def csv_siren_siret():
    return """id<sep>siren<sep>siret
a<sep>130025265<sep>13002526500013
b<sep>522816651<sep>52281665100056
"""


@pytest.fixture
def csv_numeric():
    return """id<sep>value
a<sep>2
b<sep>4
c<sep>12
"""


@pytest.fixture
def csv_top():
    return """cat<sep>value
a<sep>15
b<sep>13
c<sep>11
a<sep>9
"""


@pytest.fixture
def csv_custom_types_double_cr():
    """
    This is clearly an invalid file (double CR)
    but it tests an interesting case: None values in
    columns detected as custom types.

    In this case we'd rather display empty lines and None
    values than break.
    """
    return """id<sep>siren<sep>siret<sep>time\r\r
a<sep>13002526a5<sep>13002526500013<sep>12:30\r\r
b<sep>522816651<sep>52281665100056<sep>15:50\r\r
"""


def random_url():
    return f"https://example.com/{uuid.uuid4()}.csv"


@pytest_asyncio.fixture
async def uploaded_csv(rmock, csv, client):
    content = csv.replace('<sep>', ';').encode('utf-8')
    rmock.get(MOCK_CSV_URL, body=content)
    await client.get(f"/apify?url={MOCK_CSV_URL}")


async def test_apify_no_url(rmock, csv, client):
    res = await client.get('/apify')
    assert res.status_code == 400


async def test_apify_wrong_url(rmock, csv, client):
    res = await client.get('/apify?url=notanurl')
    assert res.status_code == 400


async def test_apify(rmock, csv, client):
    rmock.get(MOCK_CSV_URL, status=200, body=csv.encode('utf-8'))
    res = await client.get(f"/apify?url={MOCK_CSV_URL}")
    assert res.status_code == 200
    jsonres = await res.json
    assert jsonres['ok']
    assert 'endpoint' in jsonres
    assert f"/api/{MOCK_CSV_HASH}" in jsonres['endpoint']
    db_path = Path(DB_ROOT_DIR) / f"{MOCK_CSV_HASH}.db"
    assert db_path.exists()


async def test_apify_not_found(rmock, csv, client):
    rmock.get(MOCK_CSV_URL, status=404)
    res = await client.get(f"/apify?url={MOCK_CSV_URL}")
    assert res.status_code == 500
    jsonres = await res.json
    assert not jsonres['ok']
    assert jsonres['error'].startswith("Error parsing CSV: 404, message='Not Found'")


async def test_apify_w_cache(app, rmock, csv, client):
    app.config.update({'CSV_CACHE_ENABLED': True})
    rmock.get(MOCK_CSV_URL, body=csv.encode('utf-8'))
    res = await client.get(f"/apify?url={MOCK_CSV_URL}")
    assert res.status_code == 200
    jsonres = await res.json
    assert jsonres['ok']
    assert 'endpoint' in jsonres
    assert f"/api/{MOCK_CSV_HASH}" in jsonres['endpoint']
    db_path = Path(DB_ROOT_DIR) / f"{MOCK_CSV_HASH}.db"
    assert db_path.exists()
    app.config.update({'CSV_CACHE_ENABLED': False})


async def test_apify_col_mismatch(rmock, csv_col_mismatch, client):
    rmock.get(MOCK_CSV_URL, body=csv_col_mismatch.replace('<sep>', ';').encode('utf-8'))
    res = await client.get(f"/apify?url={MOCK_CSV_URL}")
    assert res.status_code == 200
    jsonres = await res.json
    assert jsonres['ok']


async def test_apify_hour_format(rmock, csv_hour, client):
    content = csv_hour.replace('<sep>', ';').encode('utf-8')
    url = random_url()
    rmock.get(url, body=content)
    await client.get(f"/apify?url={url}")
    res = await client.get(f"/api/{get_hash(url)}")
    assert res.status_code == 200
    jsonres = await res.json
    assert jsonres['columns'] == ['rowid', 'id', 'hour']
    assert jsonres['total'] == 3
    assert jsonres['rows'] == [
        [1, 'a', '12:30'],
        [2, 'b', '9:15'],
        [3, 'c', '09:45'],
    ]


async def test_apify_siren_siret_format(rmock, csv_siren_siret, client):
    content = csv_siren_siret.replace('<sep>', ';').encode('utf-8')
    url = random_url()
    rmock.get(url, body=content)
    await client.get(f"/apify?url={url}")
    res = await client.get(f"/api/{get_hash(url)}")
    assert res.status_code == 200
    jsonres = await res.json
    assert jsonres['columns'] == ['rowid', 'id', 'siren', 'siret']
    assert jsonres['total'] == 2
    assert jsonres['rows'] == [
        [1, 'a', '130025265', '13002526500013'],
        [2, 'b', '522816651', '52281665100056'],
    ]


async def test_apify_custom_types_double_cr(rmock, csv_custom_types_double_cr, client):
    content = csv_custom_types_double_cr.replace('<sep>', ';').encode('utf-8')
    url = random_url()
    rmock.get(url, body=content)
    await client.get(f"/apify?url={url}")
    res = await client.get(f"/api/{get_hash(url)}")
    assert res.status_code == 200
    jsonres = await res.json
    assert jsonres['columns'] == ['rowid', 'id', 'siren', 'siret', 'time']
    assert jsonres['total'] == 5
    assert jsonres['rows'] == [
        [1, None, None, None, None],
        [2, 'a', '13002526a5', '13002526500013', '12:30'],
        [3, None, None, None, None],
        [4, 'b', '522816651', '52281665100056', '15:50'],
        [5, None, None, None, None]
    ]


@pytest.mark.parametrize('separator', [';', ',', '\t'])
@pytest.mark.parametrize('encoding', ['utf-8', 'iso-8859-15', 'iso-8859-1'])
async def test_api(client, rmock, csv, separator, encoding):
    content = csv.replace('<sep>', separator).encode(encoding)
    rmock.get(MOCK_CSV_URL, body=content)
    await client.get(f"/apify?url={MOCK_CSV_URL}")
    res = await client.get(f"/api/{MOCK_CSV_HASH}")
    assert res.status_code == 200
    jsonres = await res.json
    assert jsonres['columns'] == ['rowid', 'col a', 'col b', 'col c']
    assert jsonres['total'] == 2
    assert jsonres['rows'] == [
        [1, 'data à1', 'data b1', 'z'],
        [2, 'data ª2', 'data b2', 'a'],
    ]


async def test_api_limit(client, rmock, uploaded_csv):
    res = await client.get(f"/api/{MOCK_CSV_HASH}?_size=1")
    assert res.status_code == 200
    jsonres = await res.json
    assert len(jsonres['rows']) == 1
    assert jsonres['rows'] == [
        [1, 'data à1', 'data b1', 'z'],
    ]


async def test_api_limit_offset(client, rmock, uploaded_csv):
    res = await client.get(f"/api/{MOCK_CSV_HASH}?_size=1&_offset=1")
    assert res.status_code == 200
    jsonres = await res.json
    assert len(jsonres['rows']) == 1
    assert jsonres['rows'] == [
        [2, 'data ª2', 'data b2', 'a'],
    ]


async def test_api_wrong_limit(client, rmock, uploaded_csv):
    res = await client.get(f"/api/{MOCK_CSV_HASH}?_size=toto")
    assert res.status_code == 400


async def test_api_wrong_shape(client, rmock, uploaded_csv):
    res = await client.get(f"/api/{MOCK_CSV_HASH}?_shape=toto")
    assert res.status_code == 400


async def test_api_objects_shape(client, rmock, uploaded_csv):
    res = await client.get(f"/api/{MOCK_CSV_HASH}?_shape=objects")
    assert res.status_code == 200
    jsonres = await res.json
    assert jsonres['rows'] == [
        {
            'rowid': 1,
            'col a': 'data à1',
            'col b': 'data b1',
            'col c': 'z',
        }, {
            'rowid': 2,
            'col a': 'data ª2',
            'col b': 'data b2',
            'col c': 'a',
        }
    ]


async def test_api_objects_norowid(client, rmock, uploaded_csv):
    res = await client.get(f"/api/{MOCK_CSV_HASH}?_shape=objects&_rowid=hide")
    assert res.status_code == 200
    jsonres = await res.json
    assert jsonres['rows'] == [
        {
            'col a': 'data à1',
            'col b': 'data b1',
            'col c': 'z',
        }, {
            'col a': 'data ª2',
            'col b': 'data b2',
            'col c': 'a',
        }
    ]


async def test_api_objects_nototal(client, rmock, uploaded_csv):
    res = await client.get(f"/api/{MOCK_CSV_HASH}?_total=hide")
    assert res.status_code == 200
    jsonres = await res.json
    assert jsonres.get('total') is None


async def test_api_sort(client, rmock, uploaded_csv):
    res = await client.get(f"/api/{MOCK_CSV_HASH}?_sort=col c")
    assert res.status_code == 200
    jsonres = await res.json
    assert jsonres['rows'] == [
        [2, 'data ª2', 'data b2', 'a'],
        [1, 'data à1', 'data b1', 'z'],
    ]


async def test_api_sort_desc(client, rmock, uploaded_csv):
    res = await client.get(f"/api/{MOCK_CSV_HASH}?_sort_desc=col b")
    assert res.status_code == 200
    jsonres = await res.json
    assert jsonres['rows'] == [
        [2, 'data ª2', 'data b2', 'a'],
        [1, 'data à1', 'data b1', 'z'],
    ]


async def test_apify_file_too_big(app, client, rmock):
    original_max_file_size = app.config.get('MAX_FILE_SIZE')
    app.config.update({'MAX_FILE_SIZE': 1})
    here = os.path.dirname(os.path.abspath(__file__))
    content = open(f"{here}/samples/test.{'xls'}", 'rb')
    mock_url = MOCK_CSV_URL.replace('.csv', 'xls')
    rmock.get(mock_url, body=content.read())
    content.close()
    res = await client.get(f"/apify?url={mock_url}")
    assert res.status_code == 500
    jsonres = await res.json
    assert 'File too big' in jsonres['error']
    app.config.update({'MAX_FILE_SIZE': original_max_file_size})


@pytest.mark.parametrize('extension', ['xls', 'xlsx'])
async def test_api_excel(client, rmock, extension):
    here = os.path.dirname(os.path.abspath(__file__))
    content = open(f"{here}/samples/test.{extension}", 'rb')
    mock_url = MOCK_CSV_URL.replace('.csv', extension)
    mock_hash = get_hash(mock_url)
    rmock.get(mock_url, body=content.read())
    content.close()
    await client.get(f"/apify?url={mock_url}")
    res = await client.get(f"/api/{mock_hash}")
    assert res.status_code == 200
    jsonres = await res.json
    assert jsonres['columns'] == ['rowid', 'col a', 'col b', 'col c']
    assert jsonres['rows'] == [
        [1, 'a1', 'b1', 'z'],
        [2, 'a2', 'b2', 'a'],
    ]


async def test_api_filter_referrers(app, client):
    app.config.update({'REFERRERS_FILTER': ['toto.com']})
    res = await client.get(f"/api/{'404'}")
    assert res.status_code == 403
    res = await client.get(f"/apify?url={'http://toto.com'}")
    assert res.status_code == 403
    res = await client.get(f"/api/{'404'}", headers={'Referer': 'http://next.toto.com'})
    assert res.status_code == 404
    app.config.update({'REFERRERS_FILTER': None})


@pytest.mark.parametrize('csv_path', Path(__file__).parent.glob('samples/real_csv/*.csv'))
async def test_real_csv_files(client, rmock, csv_path):
    with open(csv_path, 'rb') as content:
        rmock.get(MOCK_CSV_URL, body=content.read())
    res = await client.get(f"/apify?url={MOCK_CSV_URL}")
    assert res.status_code == 200
    res = await client.get(f"/api/{MOCK_CSV_HASH}")
    # w/ no error and more than 1 column and row we should be OK
    assert res.status_code == 200
    jsonres = await res.json
    assert len(jsonres['columns']) > 1
    assert len(jsonres['rows']) > 1


@pytest.mark.parametrize('xls_path', Path(__file__).parent.glob('samples/real_xls/*.xls*'))
async def test_real_xls_files(client, rmock, xls_path):
    with open(xls_path, 'rb') as content:
        rmock.get(MOCK_CSV_URL, body=content.read())
    res = await client.get(f"/apify?url={MOCK_CSV_URL}")
    assert res.status_code == 200
    res = await client.get(f"/api/{MOCK_CSV_HASH}")
    # w/ no error and more than 1 column and row we should be OK
    assert res.status_code == 200
    jsonres = await res.json
    assert len(jsonres['columns']) > 0
    assert len(jsonres['rows']) > 0


@pytest_asyncio.fixture
async def uploaded_csv_filters(rmock, csv_filters, client):
    content = csv_filters.encode('utf-8')
    rmock.get(MOCK_CSV_URL_FILTERS, body=content)
    await client.get(f"/apify?url={MOCK_CSV_URL_FILTERS}")


async def test_api_filters_exact_hour(rmock, uploaded_csv_filters, client):
    res = await client.get(f"/api/{MOCK_CSV_HASH_FILTERS}?hour__exact=12:30")
    assert res.status_code == 200
    jsonres = await res.json
    assert jsonres['total'] == 1
    assert jsonres['rows'] == [
        [1, 'first', '12:30', 1.0, 'value'],
    ]


async def test_api_filters_contains_string(rmock, uploaded_csv_filters, client):
    res = await client.get(f"/api/{MOCK_CSV_HASH_FILTERS}?id__contains=fir")
    assert res.status_code == 200
    jsonres = await res.json
    assert jsonres['total'] == 1
    assert jsonres['rows'] == [
        [1, 'first', '12:30', 1.0, 'value'],
    ]


async def test_api_filters_contains_exact_int(rmock, uploaded_csv_filters, client):
    "NB: suboptimal API result, int value returns a float"
    res = await client.get(f"/api/{MOCK_CSV_HASH_FILTERS}?value__exact=1")
    assert res.status_code == 200
    jsonres = await res.json
    assert jsonres['total'] == 1
    assert jsonres['rows'] == [
        [1, 'first', '12:30', 1.0, 'value'],
    ]


async def test_api_filters_contains_exact_float(rmock, uploaded_csv_filters, client):
    res = await client.get(f"/api/{MOCK_CSV_HASH_FILTERS}?value__exact=1.0")
    assert res.status_code == 200
    jsonres = await res.json
    assert jsonres['total'] == 1
    assert jsonres['rows'] == [
        [1, 'first', '12:30', 1.0, 'value'],
    ]


async def test_api_and_filters(rmock, uploaded_csv_filters, client):
    res = await client.get(f"/api/{MOCK_CSV_HASH_FILTERS}?id__contains=fir&value__exact=1")
    assert res.status_code == 200
    jsonres = await res.json
    assert jsonres['total'] == 1
    assert jsonres['rows'] == [
        [1, 'first', '12:30', 1.0, 'value'],
    ]


async def test_api_filters_greater_float(rmock, csv_numeric, client):
    content = csv_numeric.replace('<sep>', ';').encode('utf-8')
    url = random_url()
    rmock.get(url, body=content)
    await client.get(f"/apify?url={url}")
    res = await client.get(f"/api/{get_hash(url)}?value__greater=10")
    assert res.status_code == 200
    jsonres = await res.json
    print(jsonres)
    assert jsonres['rows'] == [
        [3, 'c', 12],
    ]


async def test_api_filters_less_float(rmock, csv_numeric, client):
    content = csv_numeric.replace('<sep>', ';').encode('utf-8')
    url = random_url()
    rmock.get(url, body=content)
    await client.get(f"/apify?url={url}")
    res = await client.get(f"/api/{get_hash(url)}?value__less=3")
    assert res.status_code == 200
    jsonres = await res.json
    print(jsonres)
    assert jsonres['rows'] == [
        [1, 'a', 2],
    ]


async def test_api_filters_less_greater_float(rmock, csv_numeric, client):
    content = csv_numeric.replace('<sep>', ';').encode('utf-8')
    url = random_url()
    rmock.get(url, body=content)
    await client.get(f"/apify?url={url}")
    res = await client.get(f"/api/{get_hash(url)}?value__greater=3&value__less=10")
    assert res.status_code == 200
    jsonres = await res.json
    assert jsonres['rows'] == [
        [2, 'b', 4],
    ]

async def test_api_filters_less_greater_string_error(rmock, csv_numeric, client):
    content = csv_numeric.replace('<sep>', ';').encode('utf-8')
    url = random_url()
    rmock.get(url, body=content)
    await client.get(f"/apify?url={url}")
    res = await client.get(f"/api/{get_hash(url)}?value__greater=3&value__less=stan")
    assert res.status_code == 400
    jsonres = await res.json
    assert jsonres == {"error":"Float value expected for less comparison.", "error_id": None , "ok":False}


async def test_api_filters_unnormalized_column(rmock, uploaded_csv_filters, client):
    res = await client.get(f"/api/{MOCK_CSV_HASH_FILTERS}?id__contains=fir&another column__contains=value")
    assert res.status_code == 200
    jsonres = await res.json
    assert jsonres['total'] == 1
    assert jsonres['rows'] == [
        [1, 'first', '12:30', 1.0, 'value'],
    ]


async def test_apify_analysed_format_response(rmock, csv_siren_siret, client):
    content = csv_siren_siret.replace('<sep>', ';').encode('utf-8')
    url = random_url()
    rmock.get(url, body=content)
    await client.get(f"/apify?url={url}&analysis=yes")
    res = await client.get(f"/api/{get_hash(url)}")
    assert res.status_code == 200
    jsonres = await res.json
    assert all(x in jsonres['columns_infos'] for x in ['id', 'siren', 'siret'])
    assert all(x in jsonres['general_infos'] for x in [
        'dataset_id',
        'date_last_check',
        'encoding',
        'header_row_idx',
        'nb_cells_missing',
        'nb_columns',
        'nb_vars_all_missing',
        'nb_vars_with_missing',
        'resource_id',
        'separator',
        'total_lines',
        'filetype'
    ])


async def test_apify_analysed_csv_detective_check_format(rmock, csv_siren_siret, client):
    content = csv_siren_siret.replace('<sep>', ';').encode('utf-8')
    url = random_url()
    rmock.get(url, body=content)
    await client.get(f"/apify?url={url}&analysis=yes")
    res = await client.get(f"/api/{get_hash(url)}")
    assert res.status_code == 200
    jsonres = await res.json
    assert jsonres['columns_infos']['siren']['format'] == 'siren'
    assert jsonres['columns_infos']['siret']['format'] == 'siret'


async def test_apify_analysed_pandas_profiling_check_numeric(rmock, csv_numeric, client):
    content = csv_numeric.replace('<sep>', ';').encode('utf-8')
    url = random_url()
    rmock.get(url, body=content)
    await client.get(f"/apify?url={url}&analysis=yes")
    res = await client.get(f"/api/{get_hash(url)}")
    assert res.status_code == 200
    jsonres = await res.json
    assert jsonres['columns_infos']['value']['numeric_infos']['max'] == 12
    assert jsonres['columns_infos']['value']['numeric_infos']['min'] == 2
    assert jsonres['columns_infos']['value']['numeric_infos']['mean'] == 6


async def test_apify_analysed_pandas_profiling_check_top(rmock, csv_top, client):
    content = csv_top.replace('<sep>', ';').encode('utf-8')
    url = random_url()
    rmock.get(url, body=content)
    await client.get(f"/apify?url={url}&analysis=yes")
    res = await client.get(f"/api/{get_hash(url)}")
    assert res.status_code == 200
    jsonres = await res.json
    assert jsonres['columns_infos']['cat']['top_infos'][0]['value'] == 'a'


async def test_apify_analysed_check_general_infos(rmock, csv_top, client):
    content = csv_top.replace('<sep>', ';').encode('utf-8')
    url = random_url()
    rmock.get(url, body=content)
    await client.get(f"/apify?url={url}&analysis=yes")
    res = await client.get(f"/api/{get_hash(url)}")
    assert res.status_code == 200
    jsonres = await res.json
    assert jsonres['general_infos']['nb_columns'] == 2
    assert jsonres['general_infos']['total_lines'] == 4
    assert jsonres['general_infos']['separator'] == ';'
    assert jsonres['general_infos']['header_row_idx'] == 0


@pytest.mark.parametrize('extension', ['xls', 'xlsx'])
async def test_no_analysis_when_excel(client, rmock, extension):
    here = os.path.dirname(os.path.abspath(__file__))
    content = open(f"{here}/samples/test.{extension}", 'rb')
    mock_url = MOCK_CSV_URL.replace('.csv', extension)
    mock_hash = get_hash(mock_url)
    rmock.get(mock_url, body=content.read())
    content.close()
    await client.get(f"/apify?url={mock_url}&analysis=yes")
    res = await client.get(f"/api/{mock_hash}")
    assert res.status_code == 200
    jsonres = await res.json
    print(jsonres)
    assert jsonres['columns'] == ['rowid', 'col a', 'col b', 'col c']
    assert jsonres['general_infos'] == { 'filetype': 'excel' }
    assert jsonres['columns_infos'] == {}