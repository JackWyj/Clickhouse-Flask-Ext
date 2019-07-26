"""
Client for ClickHouse
ref: 
    https://flask.palletsprojects.com/en/1.1.x/extensiondev/
"""

from clickhouse_driver import Client

from flask import current_app
from flask import _app_ctx_stack
import logging

logger = logging.getLogger('clickhouse.driver')


class ClickHouseDriver(object):
    def __init__(self, app=None):
        self.app = app
        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        app.config.setdefault('CLICKHOUSE_HOST', 'localhost')
        app.config.setdefault('CLICKHOUSE_PORT', 9000)
        app.config.setdefault('CLICKHOUSE_DATABASE', 'default')
        app.config.setdefault('CLICKHOUSE_USERNAME', 'default')
        app.config.setdefault('CLICKHOUSE_PASSWORD', None)
        app.config.setdefault('CLICKHOUSE_CLIENT_NAME', 'ClickHouseDriver')
        app.config.setdefault('CLICKHOUSE_CONNECT_TIMEOUT', 10)
        app.config.setdefault('CLICKHOUSE_SEND_RECEIVE_TIMEOUT', 10)
        app.config.setdefault('CLICKHOUSE_SYNC_REQUEST_TIMEOUT', 5)
        app.config.setdefault('CLICKHOUSE_COMPRESSION', False)
        app.config.setdefault('CLICKHOUSE_SECURE', False)
        app.config.setdefault('CLICKHOUSE_VERIFY', True)
        app.config.setdefault('CLICKHOUSE_SSL_VERSION', None)
        app.config.setdefault('CLICKHOUSE_CA_CERTS', None)
        app.config.setdefault('CLICKHOUSE_CIPHERS', None)
        app.teardown_appcontext(self.teardown)

    def connect(self):
        cfg = current_app.config
        logger.info('host: %s, user: %s', cfg['CLICKHOUSE_HOST'], cfg['CLICKHOUSE_USERNAME'])
        return Client(cfg['CLICKHOUSE_HOST'],
                      port=cfg['CLICKHOUSE_PORT'],
                      database=cfg['CLICKHOUSE_DATABASE'],
                      user=cfg['CLICKHOUSE_USERNAME'],
                      password=cfg['CLICKHOUSE_PASSWORD'],
                      client_name=cfg['CLICKHOUSE_CLIENT_NAME'],
                      connect_timeout=cfg['CLICKHOUSE_CONNECT_TIMEOUT'],
                      send_receive_timeout=cfg['CLICKHOUSE_SEND_RECEIVE_TIMEOUT'],
                      sync_request_timeout=cfg['CLICKHOUSE_SYNC_REQUEST_TIMEOUT'],
                      compression=cfg['CLICKHOUSE_COMPRESSION'],
                      secure=cfg['CLICKHOUSE_SECURE'],
                      verify=cfg['CLICKHOUSE_VERIFY'],
                      ssl_version=cfg['CLICKHOUSE_SSL_VERSION'],
                      ca_certs=cfg['CLICKHOUSE_CA_CERTS'],
                      ciphers=cfg['CLICKHOUSE_CIPHERS'])

    def teardown(self, _):
        ctx = _app_ctx_stack.top
        if hasattr(ctx, 'clickhouse_client'):
            ctx.clickhouse_client.disconnect()

    @property
    def connection(self):
        ctx = _app_ctx_stack.top
        if ctx is not None:
            if not hasattr(ctx, 'clickhouse_client'):
                ctx.clickhouse_client = self.connect()
            return ctx.clickhouse_client
        return None
