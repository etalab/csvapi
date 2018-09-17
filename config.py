DB_ROOT_DIR = './dbs'
CSV_CACHE_ENABLED = True
MAX_WORKERS = 3
DEBUG = True
SENTRY_DSN = None
FORCE_SSL = False
# In bytes, cf `sniff_limit` https://agate.readthedocs.io/en/1.6.1/api/table.html#agate.Table.from_csv
CSV_SNIFF_LIMIT = 4096
# In bytes, csvapi will stop downloading files if they reach this size
# Default to 100 Mo
MAX_FILE_SIZE = 1024 * 1024 * 100
