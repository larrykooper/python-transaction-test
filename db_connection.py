import logging

import psycopg2
import yaml

from amg_cursor import AmgCursor

class DbConnection(object):
    """Wrapper class for holding a Redshift database connection.
    """

    # TODO: move the logic to the AmgCursor class and have DbConnection call
    # the cursor code. This will allow the caller to customize how their
    # transactions are grouped.

    DEFAULT_AUTOCOMMIT = True

    @classmethod
    def from_yaml(cls, file_path, *yaml_scope):
        """Returns new DbConnection instance from the yaml file.
        Required values: host, database, user, password, port
        Optional values: autocommit
        :param file_path: path to the yaml configuration file.
        :param yaml_scope: the section of the yaml file to look into.
        :return: a new DbConnection instance.
        """
        logging.debug(
            'Creating DbConnection from %s[%s]',
            file_path,
            '->'.join(yaml_scope))

        info = yaml.safe_load(file(file_path))
        for scope in yaml_scope:
            info = info[scope]

        return DbConnection(
            host=info['host'],
            database=info['database'],
            user=info.get('username', ''),
            password=info['password'],
            port=info.get('port', 5432),
            autocommit=info.get('autocommit', cls.DEFAULT_AUTOCOMMIT))

    def __init__(
            self,
            host,
            database,
            user,
            password,
            port,
            autocommit=DEFAULT_AUTOCOMMIT):
        # TODO: add statement_timeout option
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.autocommit = autocommit
        self.cache = {}  # Cache for holding some query results.
        self.conn = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def is_connected(self):
        """Returns true if the database is actively connected."""
        return self.conn is not None and self.conn.closed == 0

    def connect(self):
        """Reconnects the database connection."""
        if self.is_connected():
            self.close()

        logging.info(
            'Connecting to %s:%s/%s...', self.host, self.port, self.database)
        self.conn = psycopg2.connect(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
            port=self.port,
            cursor_factory=AmgCursor)

        self.conn.autocommit = self.autocommit

    def close(self):
        """Disconnect the database connection."""
        logging.debug(
            'Closing connection to %s:%s/%s...',
            self.host,
            self.port,
            self.database)
        self.conn.close()

    def new_cursor(self):
        """Returns a new cursor from the database connection."""
        if not self.is_connected():
            logging.info('No connection to get a cursor from. Reconnecting...')
            self.connect()
        return self.conn.cursor()

    def execute_sql(self, query, params=None, return_results=False):
        """Execute arbitrary query using a new cursor.
        :param query: sql string to execute.
        :param params: dictionary of values to use in the corresponding sql.
        :param return_results: if True, function will return the results of
            the query.
        :return: the query results or None.
        """
        with self.new_cursor() as cursor:
            cursor.execute(query, params)
            results = cursor.fetchall() if return_results else None
        return results

    def execute_sql_file(self, file_path, params=None, return_results=False):
        """Execute contents of a sql file using a new cursor.
        :param file_path: path of sql file to execute.
        :param params: dictionary of values to use in the corresponding sql.
        :param return_results: if True, function will return the results of
            the query.
        :return: the query results or None.
        """
        logging.info('Executing sql file: %s', file_path)
        with open(file_path, 'r') as exec_f:
            return self.execute_sql(exec_f.read(), params, return_results)

    def execute_unload(
            self,
            select_query,
            s3_conn,
            s3_prefix,
            unload_options=()):
        """Execute a Redshift UNLOAD statement using a new cursor.
        :param select_query: query string to unload.
        :param s3_conn: S3Connection object where output will go.
        :param s3_prefix: prefix string where the files will go.
        :param unload_options: additional Redshift UNLOAD options.
        """
        s3_path = 's3://{}/{}'.format(s3_conn.bucket_name, s3_prefix)
        logging.info("Unloading query: '%s' to %s", select_query, s3_path)
        unload_string = (
            'UNLOAD (%(select_query)s) TO %(s3_path)s '
            'CREDENTIALS %(aws_creds)s')

        # Append any additional options:
        #   https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html
        # TODO: ensure opt is in a set of allowable options (for
        # security + better warnings)
        for opt in unload_options:
            unload_string += ' ' + opt

        rs_cred_string = redshift_cred_string(
            s3_conn.get_aws_access_key_id(),
            s3_conn.get_aws_secret_access_key())

        self.execute_sql(
            unload_string,
            {
                'select_query': select_query,
                's3_path': s3_path,
                'aws_creds': rs_cred_string,
            })

    def execute_copy(
            self,
            tablename,
            s3_conn,
            s3_prefix,
            columns=None,
            copy_options=()):
        """Execute a Redshift COPY statement using a new cursor.
        :param tablename: full tablename to copy to.
        :param s3_conn: S3Connection object that holds the files.
        :param s3_prefix: prefix string where the files are.
        :param columns: list of columns to use for the COPY statement.
        :param copy_options: additional Redshift COPY options.
        """
        with self.new_cursor() as cursor:
            cursor.execute_copy(
                tablename, s3_conn, s3_prefix, columns, copy_options)

    def drop_tables(self, tables, schema=None, use_if_exists=True):
        """Drops a list of tables.
        :param tables: iterable of tables to drop
        :param schema: optional schema string to append to each table name
        :param use_if_exists: use the `IF EXISTS` sql condition
        """
        if len(tables) > 0:
            if_exists = 'IF EXISTS ' if use_if_exists else ''
            if schema is not None:
                tables = ['{}.{}'.format(schema, table) for table in tables]
            query = 'DROP TABLE {}{};'.format(if_exists, ', '.join(tables))
            self.execute_sql(query)

    def get_media_import_id(
            self,
            media_schema,
            data_source,
            file_name,
            table_append=''):
        """Get the import id from the associated file_name.
        :param media_schema: schema holding the imports table.
        :param data_source: restrict searching for a data source ('COMSCORE',
            'FYI', etc...)
        :param file_name: filename to get the id for.
        :return: the id associated with the file_name. None if not found.
        """
        # Identifier for the cached import ids.
        cache_key = 'filename_to_import_id'
        data_source = data_source.upper()

        if cache_key in self.cache and file_name in self.cache[cache_key]:
            # If we already have the import id cached, no need to run the
            # query.
            return self.cache[cache_key][file_name]
        else:
            query = """SELECT id FROM %(media_schema)s.imports%(table_append)s
            WHERE source = %(data_source)s AND file_name = %(file_name)s"""
            res = self.execute_sql(
                query,
                {
                    'media_schema': no_quotes(media_schema),
                    'data_source': data_source,
                    'file_name': file_name,
                    'table_append': no_quotes(table_append),
                },
                return_results=True)

            if len(res) == 0:
                logging.info('No associated import id found for %s', file_name)
                return None
            elif len(res) > 1:
                msg = (
                    '{} is associated with multiple import records: {}'.format(
                        file_name, res))
                logging.critical(msg)
                raise Exception(msg)
            else:
                import_id = res[0]['id']
                # Cache the import_id for future calls.
                if cache_key not in self.cache:
                    self.cache[cache_key] = {}
                self.cache[cache_key][file_name] = import_id
                return import_id

    def update_media_import(
            self,
            media_schema,
            data_source,
            file_name,
            file_date,
            status,
            table_append='',
            file_path=''):
        """Update or insert an entry in media.imports.
        :param media_schema: schema holding the imports table.
        :param data_source: data source of the file_name ('COMSCORE',
            'FYI', etc...)
        :param file_name: filename to update the imports record for
        :param file_date: date associated with the file_name
        :param status: new status for the imports record
        :return: the import id that was created or found. None if error.
        """
        data_source = data_source.upper()
        import_id = self.get_media_import_id(
            media_schema, data_source, file_name, table_append)

        if import_id is None and (status.upper() in ('STARTED', 'SKIPPED')):
            # New 'STARTED' imports record.
            query = """
                INSERT INTO %(media_schema)s.imports%(table_append)s
                    (file_name, source, file_date, status, file_path)
                VALUES (%(file_name)s, %(data_source)s,
                    %(file_date)s, %(status)s, %(file_path)s);"""
            self.execute_sql(
                query,
                {
                    'media_schema': no_quotes(media_schema),
                    'file_name': file_name,
                    'data_source': data_source,
                    'file_date': file_date,
                    'status': status.upper(),
                    'table_append': no_quotes(table_append),
                    'file_path': file_path,
                })

            import_id = self.get_media_import_id(
                media_schema, data_source, file_name, table_append)
        elif import_id is None:
            # Only 'STARTED' is a valid entry for a new imports record
            raise Exception(
                'Cannot change status of {} to {}. No record '
                'exists yet.'.format(file_name, status))
        else:
            # Import record exists. Update it to the new status.
            query = (
                'UPDATE %(media_schema)s.imports%(table_append)s '
                'SET status = %(status)s')
            if status == 'STARTED':
                # STARTED records get timestamped.
                query += ', time_imported = GETDATE()'
            query += ' WHERE id = %(import_id)s;'
            self.execute_sql(
                query,
                {
                    'media_schema': no_quotes(media_schema),
                    'status': status,
                    'import_id': import_id,
                    'table_append': no_quotes(table_append),
                })
        return import_id

    def update_media_imports(
            self,
            import_ids_list,
            new_status,
            media_schema,
            table_append=''):
        with self.new_cursor() as cursor:
            cursor.update_media_imports(
                import_ids_list, new_status, media_schema, table_append)

    def get_imported_files(self, media_schema, data_source, table_append=""):
        """Returns set of file names marked with 'SUCCESS' as their status.
        :param media_schema: schema holding the imports table.
        :param data_source: data source to restrict results to ('COMSCORE',
            'FYI', etc...)
        :return: set of relevant file names.
        """
        data_source = data_source.upper()

        query = (
            "SELECT file_name FROM %(media_schema)s.imports%(table_append)s "
            "WHERE source = %(data_source)s AND (status = 'SUCCESS' OR "
            "status = 'SKIPPED');")
        res = self.execute_sql(
            query,
            {
                'media_schema': no_quotes(media_schema),
                'data_source': data_source,
                'table_append': no_quotes(table_append),
            },
            return_results=True)

        return set(row['file_name'] for row in res)

    def run_ingest_queries(
            self,
            media_schema,
            data_source,
            sql_file,
            params,
            list_of_files,
            table_append=''):
        """Run the sql file and mark the files as 'SUCCEEDED'. Used to run
            final ingestion queries.
        :param media_schema: schema holding the imports table.
        :param data_source: data source to restrict results to ('COMSCORE',
            'FYI', etc...)
        :param sql_file: path of sql file to execute.
        :param params: dictionary of values to use in the corresponding sql.
        :param list_of_files: list of filenames to update the imports
            records for
        """
        data_source = data_source.upper()
        status = 'UNKNOWN'
        try:
            self.execute_sql_file(sql_file, params)
            status = 'SUCCESS'
        except psycopg2.Error as err:
            logging.error('Failed to run ingest queries with error %s', err)
            status = 'FAIL'
            raise
        finally:
            for file_name, file_date in list_of_files:
                self.update_media_import(
                    media_schema,
                    data_source,
                    file_name,
                    file_date,
                    status,
                    table_append)