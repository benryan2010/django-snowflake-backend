from django.conf import settings
from django.db.backends.base.operations import BaseDatabaseOperations
import snowflake.connector as Database


class DatabaseOperations(BaseDatabaseOperations):

    def bulk_insert_sql(self, fields, placeholder_rows):
        placeholder_rows_sql = (", ".join(row) for row in placeholder_rows)
        values_sql = ", ".join("(%s)" % sql for sql in placeholder_rows_sql)
        return "VALUES " + values_sql

    def date_extract_sql(self, lookup_type, field_name):
        pass

    def date_interval_sql(self, timedelta):
        pass

    def date_trunc_sql(self, lookup_type, field_name):
        pass

    def datetime_cast_date_sql(self, field_name, tzname):
        pass

    def datetime_cast_time_sql(self, field_name, tzname):
        pass

    def datetime_extract_sql(self, lookup_type, field_name, tzname):
        pass

    def datetime_trunc_sql(self, lookup_type, field_name, tzname):
        pass

    def last_insert_id(self, cursor, table_name, pk_name):

        """
        Given a cursor object that has just performed an INSERT statement into
        a table that has an auto-incrementing ID, return the newly created ID.

        `pk_name` is the name of the primary-key column.
        """

        # snowflake doesn't natively support returning last inserted id, so a workaround from
        # https://stackoverflow.com/questions/53837950#53903693 that is endorsed by snowflake customer support
        # has to be used involving MAX() an snowflake timetravel. Two queries are required because snowflake
        # python sdk doesn't support multiple queries in a single execution

        try:
            with self.connection.cursor() as cursor:
                cursor.execute('SET qid = last_query_id()')
        # TODO: map exception here
        except Database.errors.ProgrammingError as e:
            # default error message
            print(e)
            # customer error message
            print('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
            return False

        set_statement_id_sql = 'select max("{0}") from "{1}" AT(statement=>$qid)'.format(pk_name, table_name)

        try:
            with self.connection.cursor() as cursor:
                last_row_id, = cursor.execute(set_statement_id_sql).fetchone()
                return last_row_id
        # TODO: map exception here
        except Database.errors.ProgrammingError as e:
            # default error message
            print(e)
            # customer error message
            print('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
            return False

    def no_limit_value(self):
        pass

    def quote_name(self, name):
        if name.startswith('"') and name.endswith('"'):
            return name  # Quoting once is enough.
        return '"%s"' % name

    def regex_lookup(self, lookup_type):
        pass

    def return_insert_columns(self, fields):
        return None

    def time_trunc_sql(self, lookup_type, field_name):
        pass

    def sql_flush(self, style, tables, *, reset_sequences=False, allow_cascade=False):
        pass
