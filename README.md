# csvapi

"Instantly" publish an API for a CSV hosted anywhere on the internet.

## Installation

Requires Python 3.6+.

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

## Credits

Inspired by the excellent [Datasette](https://github.com/simonw/datasette).
