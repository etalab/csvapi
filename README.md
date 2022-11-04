# csvapi

"Instantly" publish an API for a CSV hosted anywhere on the internet. Also supports Excel files.

This tool is used by [data.gouv.fr](https://www.data.gouv.fr) to show a preview of hosted CSV and XLS files.

## Installation

Requires Python 3.9+ and a Unix OS with the `file` command available.

```shell
python3 -m venv pyenv && . pyenv/bin/activate
pip install csvapi
```

For development:

```shell
poetry install
```

## Quickstart

```shell
poetry run csvapi serve -h 0.0.0.0 -p 8000
```

## Command line options

```shell
$ poetry run csvapi serve --help
Usage: csvapi serve [OPTIONS]

Options:
    --ssl-key TEXT             Path to SSL key
    --ssl-cert TEXT            Path to SSL certificate
    --cache / --no-cache       Do not parse CSV again if DB already exists
    --reload                   Automatically reload if code change detected
    --debug                    Enable debug mode - useful for development
    -p, --port INTEGER         port for server, defaults to 8001
    -h, --host TEXT            host for server, defaults to 127.0.0.1
    --dbs DIRECTORY            Where to store sqlite DBs
    --help                     Show this message and exit.
```

## Deploy

With SSL, using [Hypercorn](https://pgjones.gitlab.io/hypercorn/):

```shell
hypercorn csvapi.webservice:app -b 0.0.0.0:443 --keyfile key.pem --ca-certs cert.pem
```

See [the documentation](https://pgjones.gitlab.io/hypercorn/usage.html) for more options.

You can use the environment variable `CSVAPI_CONFIG_FILE` to point to a custom configuration file.

## API usage

### Conversion

`/apify?url=http://somewhere.com/a/file.csv`

This converts a CSV to an SQLite database (w/ `agate`) and returns the following response:

```json
{"ok": true, "endpoint": "http://localhost:8001/api/cde857960e8dc24c9cbcced673b496bb"}
```

### Parameters

Some parameters can be used in the query string.

#### `encoding`

**default**: _automatic detection_

You can force an encoding (e.g. `utf-8`) using this parameter, instead of relying on the automatic detection.


### Data API

This is the `endpoint` attribute of the previous response.

`/api/<md5-url-hash>`

This queries a previously converted API file and returns the first 100 rows like this:

```json
    {
        "ok": true,
        "rows": [[], []],
        "columns": [],
        "query_ms": 1
    }
```

### Parameters

Some parameters can be used in the query string.

#### `_size`

**default**: `100`

This will limit the query to a certain number of rows. For instance to get only 250 rows:

`/api/<md5-url-hash>?_size=250`

#### `_sort` and `_sort_desc`

Use those to sort by a column. `sort` will sort by ascending order, `sort_desc` by descending order.

`/api/<md5-url-hash>?_sort=<column-name>`

#### `_offset`

Use this to add on offset. Combined with `_size` it allows pagination.

`/api/<md5-url-hash>?_size=1&_offset=1`

#### `_shape`

**default**: `lists`

The `_shape` argument is used to specify the format output of the json. It can take the value `objects` to get an array of objects instead of an array of arrays:

`/api/<md5-url-hash>?_shape=objects`

For instance, instead of returning:

```json
{
    "ok": true,
    "query_ms": 0.4799365997,
    "rows": [
        [1, "Justice", "0101", 57663310],
        [2, "Justice", "0101", 2255129],
        [3, "Justice", "0101", 36290]
    ],
    "columns": ["rowid", "Mission", "Programme", "Consommation de CP"]
}
```

It will return:

```json
{
    "ok": true,
    "query_ms": 2.681016922,
    "rows": [
    {
        "rowid": 1,
        "Mission": "Justice",
        "Programme": "0101",
        "Consommation de CP": 57663310
    },
    {
        "rowid": 2,
        "Mission": "Justice",
        "Programme": "0101",
        "Consommation de CP": 2255129
    },
    {
        "rowid": 3,
        "Mission": "Justice",
        "Programme": "0101",
        "Consommation de CP": 36290
    }],
    "columns": ["rowid", "Mission", "Programme", "Consommation de CP"]
}
```

#### `_rowid`

**default**: `show`

The `_rowid` argument is used to display or hide rowids in the returned data. Use `_rowid=hide` to hide.

`/api/<md5-url-hash>?_shape=objects&_rowid=hide`

```json
{
    "ok": true,
    "query_ms": 2.681016922,
    "rows": [
    {
        "Mission": "Justice",
        "Programme": "0101",
        "Consommation de CP": 57663310
    },
    {
        "Mission": "Justice",
        "Programme": "0101",
        "Consommation de CP": 2255129
    },
    {
        "Mission": "Justice",
        "Programme": "0101",
        "Consommation de CP": 36290
    }],
    "columns": ["Mission", "Programme", "Consommation de CP"]
}
```

#### `_total`

**default**: `show`

The `_total` argument is used to display or hide the total number of rows (independent of pagination) in the returned data. Use `_total=hide` to hide.

```json
{
    "ok": true,
    "query_ms": 2.681016922,
    "rows": [
    {
        "Mission": "Justice",
        "Programme": "0101",
        "Consommation de CP": 57663310
    },
    {
        "Mission": "Justice",
        "Programme": "0101",
        "Consommation de CP": 2255129
    },
    {
        "Mission": "Justice",
        "Programme": "0101",
        "Consommation de CP": 36290
    }],
    "columns": ["Mission", "Programme", "Consommation de CP"],
    "total": 3
}
```

#### Column based filters

By adding `{column}__{comparator}={value}` to the query string, you can filter the results based on the following criterions:
- `{column}` must be a valid column in your CSV
- `{comparator}` is `exact` (SQL `= {value}`) or `contains` (SQL `LIKE %{value}%`)
- `{value}` is the value you're filtering the column against

You can add multiple filters, they will be joined with a `AND` at the SQL level.

## Credits

Inspired by the excellent [Datasette](https://github.com/simonw/datasette).
