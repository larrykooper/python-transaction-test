import logging
import textwrap
import psycopg2
import psycopg2.extras

class AmgCursor(psycopg2.extras.DictCursor):
    """Adds additional logic to the psycopg2 DictCursor."""

    def execute(self, query, q_vars=None):
        final_sql = textwrap.dedent(self.mogrify(query, q_vars))
        logging.debug('Executing sql query:\n%s', final_sql)
        super(AmgCursor, self).execute(final_sql)

    def execute_file(self, file_path, params=None):
        """Execute sql statement inside a file.
        :param file_path: path of sql file to execute.
        :param params: dictionary of values to use in the corresponding sql.
        """
        logging.info('Executing sql file: %s', file_path)
        with open(file_path, 'r') as exec_f:
            self.execute(exec_f.read(), params)

    def execute_copy(
            self,
            tablename,
            s3_conn,
            s3_prefix,
            columns=None,
            copy_options=()):
        """Execute a Redshift COPY statement.
        :param tablename: full tablename to copy to.
        :param s3_conn: S3Connection object that holds the files.
        :param s3_prefix: prefix string where the files are.
        :param columns: list of columns to use for the COPY statement.
        :param copy_options: additional Redshift COPY options.
        """
        s3_path = 's3://{}/{}'.format(s3_conn.bucket_name, s3_prefix)
        logging.info('Copying from: %s to %s', s3_path, tablename)
        col_string = '({})'.format(
            ', '.join(columns)) if columns is not None else ''
        copy_string = (
            'COPY %(tablename)s %(columns)s FROM %(s3_path)s '
            'CREDENTIALS %(aws_creds)s')

        # Append any additional options.
        # TODO: ensure opt is in a set of allowable options (for
        # security + better warnings)
        for opt in copy_options:
            copy_string += ' ' + opt
        rs_cred_string = redshift_cred_string(
            s3_conn.get_aws_access_key_id(),
            s3_conn.get_aws_secret_access_key())

        self.execute(
            copy_string,
            {
                'tablename': no_quotes(tablename),
                'columns': no_quotes(col_string),
                's3_path': s3_path,
                'aws_creds': rs_cred_string,
            })

    def upsert(
            self,
            source_table,
            target_table,
            uniqueness_keys,
            col_list,
            col_functions=None,
            has_timestamps=True):
        """Performs an 'upsert' - an UPDATE followed by an INSERT.
        :param source_table: table holding new staging data.
        :param target_table: table being inserted/updated into.
        :param uniqueness_keys: iterable of column names that define a
            unique record.
        :param col_list: list of column names to upsert.
            Note: the source_table and target_table must share the same column
            names.
        :param col_functions: dictionary mapping column names to SQL functions.
            i.e. {'name': ['TRIM','UPPER']} generates sql: UPPER(TRIM(name))
        :param has_timestamps: if true, will assume table has created_at and
            updated_at columns.
        """
        # TODO: ability to insert static values. ex: INSERT ... (column1,
        # column2, 'STATIC_VALUE', 42, column3)

        # Apply naming aliases to prevent ambiguity.
        target_cols = ['{}.{}'.format(target_table, col) for col in col_list]
        source_cols = ['s.{}'.format(col) for col in col_list]
        target_unique_cols = [
            '{}.{}'.format(target_table, col)
            for col in uniqueness_keys
        ]
        source_unique_cols = ['s.{}'.format(col) for col in uniqueness_keys]

        # Ensure all values in col_functions are iterable.
        col_functions = {} if col_functions is None else col_functions
        col_functions = {
            k: (v if isinstance(v, (list, tuple)) else [v])
            for k, v in col_functions.iteritems()
        }

        # Wrap any SQL functions around columns names. Ex: TRIM(UPPER(s.name))
        target_cols_fn, target_unique_cols_fn = (
            target_cols[:], target_unique_cols[:])
        source_cols_fn, source_unique_cols_fn = (
            source_cols[:], source_unique_cols[:])
        for idx, col in enumerate(col_list):
            for function in col_functions.get(col, []):
                source_cols_fn[idx] = '{}({})'.format(
                    function, source_cols_fn[idx])
                target_cols_fn[idx] = '{}({})'.format(
                    function, target_cols_fn[idx])
        for idx, col in enumerate(uniqueness_keys):
            for function in col_functions.get(col, []):
                target_unique_cols_fn[idx] = '{}({})'.format(
                    function, target_unique_cols_fn[idx])
                source_unique_cols_fn[idx] = '{}({})'.format(
                    function, source_unique_cols_fn[idx])

        unique_predicates = [
            '%s = %s' % (
                target_unique_cols_fn[idx], source_unique_cols_fn[idx])
            for idx, _ in enumerate(uniqueness_keys)
        ]
        identical_row_predicates = [
            '%s = %s' % (target_cols_fn[idx], source_cols_fn[idx])
            for idx, _ in enumerate(col_list)
        ]
        update_clauses = [
            '%s = %s' % (col_list[idx], source_cols_fn[idx])
            for idx in range(len(col_list))
        ]
        if has_timestamps:
            update_clauses.append('updated_at = GETDATE()')
            col_list += ['created_at', 'updated_at']
            source_cols_fn += ['GETDATE()', 'GETDATE()']
        params = {
            'source_table': no_quotes(source_table),
            'target_table': no_quotes(target_table),
            'target_cols': no_quotes(', '.join(col_list)),
            'source_cols': no_quotes(', '.join(source_cols_fn)),
            'unique_predicates': no_quotes(
                '\n                AND '.join(unique_predicates)),
            'identical_row_predicate': no_quotes(
                '\n                AND '.join(identical_row_predicates)),
            'update_clause': no_quotes(', '.join(update_clauses)),
            'a_join_key': no_quotes('{}.{}'.format(
                target_table, uniqueness_keys[0])),
        }

        logging.debug(
            'Running upsert from %s to %s', source_table, target_table)
        self.execute(
            """
            -- Update target records that are already present.
            UPDATE %(target_table)s
            SET %(update_clause)s
            FROM %(source_table)s s
            WHERE (%(unique_predicates)s)
                AND NOT (%(identical_row_predicate)s)
            """,
            params)
        logging.debug('[Upsert] Updated %s records', self.rowcount)

        self.execute(
            """
            -- Insert new records, that are not already present.
            INSERT INTO %(target_table)s (%(target_cols)s)
            SELECT DISTINCT %(source_cols)s
            FROM %(source_table)s s
            LEFT JOIN %(target_table)s ON %(unique_predicates)s
            WHERE %(a_join_key)s IS NULL
            """,
            params)
        logging.debug('[Upsert] Inserted %s new records', self.rowcount)

    def update_media_imports(
            self,
            import_ids_list,
            new_status,
            media_schema,
            table_append=''):
        sql = (
            'UPDATE %(media_schema)s.imports%(table_append)s\nSET '
            'status = %(new_status)s'
            '\nWHERE id IN %(import_ids_list)s')
        self.execute(
            sql,
            {
                'media_schema': no_quotes(media_schema),
                'new_status': new_status,
                'import_ids_list': import_ids_list,
                'table_append': no_quotes(table_append),
            })

