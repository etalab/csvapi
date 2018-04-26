# csvapi

"Instantly" publish an API for a CSV hosted anywhere on the internet.

## Installation

Requires Python 3.5+.

```
python3 -m venv pyenv && . pyenv/bin/activate
git clone git@github.com:abulte/csvapi.git && cd csvapi
pip install -e .
```

## Quickstart

```
csvapi serve -h 0.0.0.0 -p 8000
```

## API usage

### Conversion

`/apify?url=http://somewhere.com/a/file.csv`

This converts a CSV to an SQLite database (w/ `agate`) and returns the following response:

```json
{"ok": true, "endpoint": "http://localhost:8001/api/cde857960e8dc24c9cbcced673b496bb"}
```

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

#### `limit`

This will limit the query to a certain number of rows. For instance to get only 250 rows:

`/api/<md5-url-hash>?limit=250`

#### `_shape`

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

#### `_norowid`

The `_norowid` argument is used to avoid rowids in the returned data:

`/api/<md5-url-hash>?_norowid=1`

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

## Credits

Inspired by the excellent [Datasette](https://github.com/simonw/datasette).
