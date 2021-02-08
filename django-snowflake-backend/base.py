"""
Snowflake database backend for Django.

Requires snowflake connector for python
"""

import asyncio
import threading
import warnings
from contextlib import contextmanager

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.db import DatabaseError as WrappedDatabaseError, connections
from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.backends.utils import (
    CursorDebugWrapper as BaseCursorDebugWrapper,
)
from django.utils.asyncio import async_unsafe
from django.utils.functional import cached_property
from django.utils.safestring import SafeString
from django.utils.version import get_version_tuple

try:
    import snowflake.connector as Database
except ImportError as e:
    raise ImproperlyConfigured("Error loading snowflake connector module: %s" % e)

# TODO: add versioning checks

# Some of these import snowflake connector, so import them after checking if it's installed.
from .client import DatabaseClient                          # NOQA isort:skip
from .creation import DatabaseCreation                      # NOQA isort:skip
from .features import DatabaseFeatures                      # NOQA isort:skip
from .introspection import DatabaseIntrospection            # NOQA isort:skip
from .operations import DatabaseOperations                  # NOQA isort:skip
from .schema import DatabaseSchemaEditor                    # NOQA isort:skip


class DatabaseWrapper(BaseDatabaseWrapper):
    vendor = 'snowflake'
    display_name = 'Snowflake'
    data_types = {
        'AutoField': 'NUMBER(38, 0) AUTOINCREMENT START 1 INCREMENT 1',
        'BigAutoField': 'NUMBER(38, 0)',
        'BinaryField': 'BINARY',
        'BooleanField': 'BOOLEAN',
        'CharField': 'varchar(%(max_length)s)',
        'DateField': 'DATE',
        'DateTimeField': 'timestamp with time zone',
        'DecimalField': 'NUMBER(%(max_digits)s, %(decimal_places)s)',
        'DurationField': 'interval',
        'FileField': 'varchar(%(max_length)s)',
        'FilePathField': 'varchar(%(max_length)s)',
        'FloatField': 'REAL',
        'IntegerField': 'NUMBER(38, 0)',
        'BigIntegerField': 'NUMBER(38, 0)',
        'IPAddressField': 'inet',
        'GenericIPAddressField': 'inet',
        'JSONField': 'jsonb',
        'NullBooleanField': 'BOOLEAN',
        'OneToOneField': 'NUMBER(38, 0)',
        'PositiveBigIntegerField': 'NUMBER(38, 0)',
        'PositiveIntegerField': 'NUMBER(38, 0)',
        'PositiveSmallIntegerField': 'NUMBER(38, 0)',
        'SlugField': 'varchar(%(max_length)s)',
        'SmallAutoField': 'smallserial',
        'SmallIntegerField': 'NUMBER(38, 0)',
        'TextField': 'VARCHAR',
        'TimeField': 'TIME',
        'UUIDField': 'VARCHAR',
    }
    data_type_check_constraints = {
        'PositiveBigIntegerField': '"%(column)s" >= 0',
        'PositiveIntegerField': '"%(column)s" >= 0',
        'PositiveSmallIntegerField': '"%(column)s" >= 0',
    }
    operators = {
        'exact': '= %s',
        'iexact': '= UPPER(%s)',
        'contains': 'LIKE %s',
        'icontains': 'LIKE UPPER(%s)',
        'regex': '~ %s',
        'iregex': '~* %s',
        'gt': '> %s',
        'gte': '>= %s',
        'lt': '< %s',
        'lte': '<= %s',
        'startswith': 'LIKE %s',
        'endswith': 'LIKE %s',
        'istartswith': 'LIKE UPPER(%s)',
        'iendswith': 'LIKE UPPER(%s)',
    }
    pattern_esc = r"REPLACE(REPLACE(REPLACE({}, E'\\', E'\\\\'), E'%%', E'\\%%'), E'_', E'\\_')"
    pattern_ops = {
        'contains': "LIKE '%%' || {} || '%%'",
        'icontains': "LIKE '%%' || UPPER({}) || '%%'",
        'startswith': "LIKE {} || '%%'",
        'istartswith': "LIKE UPPER({}) || '%%'",
        'endswith': "LIKE '%%' || {}",
        'iendswith': "LIKE '%%' || UPPER({})",
    }

    Database = Database
    SchemaEditorClass = DatabaseSchemaEditor

    # Classes instantiated in __init__().
    client_class = DatabaseClient
    creation_class = DatabaseCreation
    features_class = DatabaseFeatures
    introspection_class = DatabaseIntrospection
    ops_class = DatabaseOperations

    def get_connection_params(self):
        settings_dict = self.settings_dict
        conn_params = {}

        if settings_dict['DATABASE']:
            conn_params['database'] = settings_dict['DATABASE']
        else:
            raise ImproperlyConfigured("Please provide a username for snowflake!")

        if settings_dict['USER']:
            conn_params['user'] = settings_dict['USER']
        else:
            raise ImproperlyConfigured("Please provide a username for snowflake!")

        if settings_dict['PASSWORD']:
            conn_params['password'] = settings_dict['PASSWORD']
        else:
            raise ImproperlyConfigured("Please provide a password for snowflake!")

        if settings_dict['ACCOUNT']:
            conn_params['account'] = settings_dict['ACCOUNT']
        else:
            raise ImproperlyConfigured("Please provide an account for snowflake!")

        if settings_dict['WAREHOUSE']:
            conn_params['warehouse'] = settings_dict['WAREHOUSE']
        else:
            raise ImproperlyConfigured("Please provide a warehouse for snowflake!")

        if settings_dict['ROLE']:
            conn_params['role'] = settings_dict['ROLE']
        else:
            raise ImproperlyConfigured("Please provide a role for snowflake!")

        if settings_dict['SCHEMA']:
            conn_params['schema'] = settings_dict['SCHEMA']
        else:
            raise ImproperlyConfigured("Please provide a schema for snowflake!")

        return conn_params

    @async_unsafe
    def get_new_connection(self, conn_params):
        connection = Database.connect(**conn_params)
        return connection

    def init_connection_state(self):
        pass

    @async_unsafe
    def create_cursor(self, name=None):
        cursor = self.connection.cursor()
        return cursor

    def _set_autocommit(self, autocommit):
        with self.wrap_database_errors:
            self.connection.autocommit = autocommit

    def is_usable(self):
        try:
            # Use a cursor directly, bypassing Django's utilities.
            with self.connection.cursor() as cursor:
                cursor.execute('SELECT current_version()')
        except Database.Error:
            return False
        else:
            return True


class CursorDebugWrapper(BaseCursorDebugWrapper):
    def copy_expert(self, sql, file, *args):
        with self.debug_sql(sql):
            return self.cursor.copy_expert(sql, file, *args)

    def copy_to(self, file, table, *args, **kwargs):
        with self.debug_sql(sql='COPY %s TO STDOUT' % table):
            return self.cursor.copy_to(file, table, *args, **kwargs)
