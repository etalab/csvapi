DB_ROOT_DIR = './dbs'
CSV_CACHE_ENABLED = True
MAX_WORKERS = 3
DEBUG = True
SENTRY_DSN = None
FORCE_SSL = False
# In bytes, cf `sniff_limit` https://agate.readthedocs.io/en/1.6.1/api/table.html#agate.Table.from_csv
CSV_SNIFF_LIMIT = 4096 * 2
# In bytes, csvapi will stop downloading files if they reach this size
# Default to 100 Mo
MAX_FILE_SIZE = 1024 * 1024 * 100
# Set this to an array of hosts to filter out calls by referer (403 returned if no match)
# It will also match subdomains
# e.g. REFERRERS_FILTER = ['data.gouv.fr'] will match 'demo.data.gouv.fr'
REFERRERS_FILTER = None
PANDAS_PROFILING_CONFIG_MIN = False
