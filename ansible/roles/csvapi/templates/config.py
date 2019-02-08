DB_ROOT_DIR = "{{csvapi__home_dbs}}"
CSV_CACHE_ENABLED = True
MAX_WORKERS = {{csvapi__workers}}
DEBUG = {{csvapi__debug}}
SENTRY_DSN = "{{csvapi__sentry_dsn}}"
{% if csvapi__ssl_certificate %}
FORCE_SSL = True
{% endif %}
CSV_SNIFF_LIMIT = {{csvapi__csv_sniff_limit}}
MAX_FILE_SIZE = {{csvapi__max_file_size}}
REFERRERS_FILTER = [
{% for referrer in csvapi__allowed_referrers %}
    '{{ referrer }}',
{% endfor %}
]
