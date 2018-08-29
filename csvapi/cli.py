import click
import ssl

from click_default_group import DefaultGroup

from csvapi.webservice import app

RESPONSE_TIMEOUT = 5 * 60  # in seconds


@click.group(cls=DefaultGroup, default='serve', default_if_no_args=True)
@click.version_option()
def cli():
    """
    csvapi!
    """


@click.option('--dbs', default='./dbs',
              type=click.Path(exists=True, file_okay=False),
              help='Where to store sqlite DBs')
@click.option('-h', '--host', default='127.0.0.1',
              help='host for server, defaults to 127.0.0.1')
@click.option('-p', '--port', default=8001,
              help='port for server, defaults to 8001')
@click.option('--debug', is_flag=True,
              help='Enable debug mode - useful for development')
@click.option('--reload', is_flag=True,
              help='Automatically reload if code change detected')
@click.option('--cache/--no-cache', default=True,
              help='Do not parse CSV again if DB already exists')
@click.option('-w', '--max-workers', default=3,
              help='Max number of ThreadPoolExecutor workers')
@click.option('--ssl-cert', default=None,
              help='Path to SSL certificate')
@click.option('--ssl-key', default=None,
              help='Path to SSL key')
@cli.command()
def serve(dbs, host, port, debug, reload, cache, max_workers, ssl_cert, ssl_key):
    ssl_context = None
    if ssl_cert and ssl_key:
        ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ssl_context.load_cert_chain(certfile=ssl_cert, keyfile=ssl_key)
    app.config.update({
        'DB_ROOT_DIR': dbs,
        'CSV_CACHE_ENABLED': cache,
        'MAX_WORKERS': max_workers,
        'DEBUG': debug,
        # TODO this probably does not exist in Quart
        'RESPONSE_TIMEOUT': RESPONSE_TIMEOUT,
    })
    app.run(host=host, port=port, debug=debug, use_reloader=reload, ssl=ssl_context)
