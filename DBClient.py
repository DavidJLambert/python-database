""" DBClient.py """
from DBInstance import *
from OutputWriter import *
import MyQueries as mq


class DBClient(object):
    """ Get text of a SQL program with bind variables, then execute it.

    Attributes:
        sql (str): the text of a SQL program with bind variables.
        bind_vars: dict or tuple containing bind variables.
        cursor: the cursor to execute this SQL on.
            I set cursor = None when cursor closed.
    """
    def __init__(self, db_instance) -> None:
        """ Constructor method for this class.

        Parameters:
            db_instance: the handle for a database instance to use.
        Returns:
        """
        # Get database cursor.
        self.db_instance = db_instance

        # Get database type.
        self.db_type = self.db_instance.get_db_type()

        # Get database library.
        self.db_lib_name = self.db_instance.get_db_lib_name()
        db_library = __import__(self.db_lib_name)
        self.db_library = db_library

        # Get database cursor.
        self.cursor = self.db_instance.create_cursor(self)

        # Initialize SQL text and bind variables.
        self.sql: str = ''
        self.bind_vars = self.db_instance.init_bind_vars()
        self.paramstyle = self.db_instance.get_paramstyle()
        return
    # End of method __init__.

    def clean_up(self) -> None:
        """ Cleans up, in preparation for deletion.

        Parameters:
        Returns:
        """
        if self.cursor is not None:
            if self.cursor.connection is not None:
                # Don't close connection, other DBClients may be using it.
                self.cursor.connection.commit()
            self.db_instance.delete_cursor(self)
            self.cursor = None
    # End of method clean_up.

    def set_sql(self, sql: str) -> None:
        """ Set text of SQL to execute.

        Parameters:
            sql (str): text of the SQL to execute.
        Returns:
        """
        self.sql: str = sql
        return
    # End of method set_sql.

    def set_bind_vars(self, bind_vars) -> None:
        """ Set bind variables.

        Parameters:
            bind_vars: the bind variables' values and maybe names too (tuple or
                       dict(), depending on database type and db library.
        Returns:
        """
        self.bind_vars = bind_vars
        return
    # End of method set_bind_vars.

    def run_sql(self) -> (list, list, int):
        """ Run the SQL, perhaps return rows and column names.

        Parameters:
        Returns:
            For SQL SELECT:
                col_names: list of the names of the columns being fetched.
                rows: list of tuples, each tuple is one row being fetched.
                row_count: number of rows fetched.
            For other types of SQL:
                None
                None
                row_count: number of rows affected.
        """
        col_names = None
        all_rows = None
        row_count = 0
        if not self.sql:
            # No sql to execute.
            pass
        else:
            try:
                # Classify SQL.
                sql_type: str = self.sql.split()[0].upper()

                # Execute SQL.
                if len(self.bind_vars) > 0:
                    if self.db_type == access:
                        print('NO BIND VARIABLES ALLOWED IN MICROSOFT ACCESS.')
                        return
                    self.cursor.execute(self.sql, self.bind_vars)
                else:
                    self.cursor.execute(self.sql)

                if self.cursor is None:
                    print('\nCursor is None.')
                    self.clean_up()
                    exit(1)
                elif sql_type in {'INSERT', 'UPDATE', 'DELETE'}:
                    self.cursor.commit()
                elif sql_type not in {'SELECT', 'PRAGMA'}:
                    # Not a CRUD statement.
                    pass
                elif sql_type in {'SELECT', 'PRAGMA'}:
                    # Fetch rows.  Fetchall for large number of rows a problem.
                    all_rows = self.cursor.fetchall()

                    # Get column names
                    col_names = [item[0] for item in self.cursor.description]

                    # In Oracle, cursor.rowcount = 0, so get row count directly.
                    row_count = len(all_rows)

                # Get row count.
                if row_count == 0:
                    row_count: int = self.cursor.rowcount

            except self.db_library.Error:
                print_stacktrace()
            finally:
                return col_names, all_rows, row_count
    # End of method run_sql.

    # THE REST OF THE METHODS ALL HAVE TO DO WITH SEEING THE DATABASE SCHEMA.

    # EVERYTHING ABOVE IS DB_TYPE INDEPENDENT.  BELOW, NOT INDEPENDENT.

    def database_table_schema(self, colsep='|') -> None:
        """ Print the schema for a table.

        Parameters:
            colsep (str): column separator to use.
        Returns:
        """
        # The query to find the tables in this user's data dictionary views.
        sql_x = mq.find_tables_sql[self.db_type]
        if skip_operation(sql_x):
            if sql_x == mq.not_implemented:
                z = mq.not_implemented
                print('\n' + z.format("TABLES", self.db_type.upper()))
            elif sql_x == mq.not_possible_sql:
                z = mq.not_possible_sql
                print(z.format(self.db_type.upper(), self.db_lib_name.upper()))
            return

        # Execute the SQL.
        # find_tables_sql has no parameters.
        self.set_sql(sql_x)
        tables_col_names, tables_rows, tables_row_count = self.run_sql()

        # Extract the list of table names.
        tables = [row[0] for row in tables_rows]

        if len(tables) == 0:
            print('You own no tables!  Nothing to see!')
            return
        elif len(tables) == 1:
            # Only one table.  Choose that table to show.
            my_table = tables[0]
            print('\nYou have only one table: {}.'.format(my_table))
        else:
            # Ask end-user to choose a table.
            prompt = '\nHere are the tables available to you:\n'
            for index, table in enumerate(tables):
                prompt += str(index + 1) + ': ' + table + '\n'
            prompt += ('Enter the number for the table you want info about,\n'
                       'Or enter "Q" to quit:\n')

            # Keep looping until valid choice made.
            while True:
                choice = input(prompt).strip().upper()
                # Interpret the choice.
                if choice == "Q":
                    print('Quitting as requested.')
                    exit(0)
                choice = int(choice)
                if choice in range(len(tables)):
                    my_table = tables[choice - 1]
                    break
                else:
                    print('Invalid choice, please try again.')
            print('\nYou chose table "{}".'.format(my_table))

        # Set up to write output.
        writer1 = OutputWriter(out_file_name='', align_col=True, col_sep=colsep)

        # Find and print columns in this table.
        sql_x = mq.find_tab_col_sql[self.db_type]
        if skip_operation(sql_x):
            if sql_x == mq.not_implemented:
                z = mq.not_implemented
                print('\n' + z.format("TABLE'S COLUMNS", self.db_type.upper()))
            elif sql_x == mq.not_possible_sql:
                z = mq.not_possible_sql
                print(z.format(self.db_type.upper(), self.db_lib_name.upper()))
        else:
            columns_col_names, columns_rows = self._find_table_columns(my_table)
            print('\nHere are the columns for table {}:'.format(my_table))
            writer1.write_rows(columns_rows, columns_col_names)
            print()

        # Find all indexes in this table.
        sql_x = mq.find_indexes_sql[self.db_type]
        if skip_operation(sql_x):
            if sql_x == mq.not_implemented:
                z = mq.not_implemented
                print('\n' + z.format("INDEXES", self.db_type.upper()))
            elif sql_x == mq.not_possible_sql:
                z = mq.not_possible_sql
                print(z.format(self.db_type.upper(), self.db_lib_name.upper()))
            return
        indexes_col_names, indexes_rows = self._find_indexes(my_table)

        # Add 'INDEX_COLUMNS' to end of col_names.
        indexes_col_names.append('INDEX_COLUMNS')

        # Go through indexes, add index_columns to end of each index/row.
        sql_x = mq.find_ind_col_sql[self.db_type]
        if skip_operation(sql_x):
            if sql_x == mq.not_implemented:
                z = mq.not_implemented
                print('\n' + z.format("INDEX'S COLUMNS", self.db_type.upper()))
            elif sql_x == mq.not_possible_sql:
                z = mq.not_possible_sql
                print(z.format(self.db_type.upper(), self.db_lib_name.upper()))
            return
        for count, index_row in enumerate(indexes_rows):
            index_name = index_row[0]
            _, ind_col_rows = self._find_index_columns(index_name)
            # Concatenate names of columns in index.  In function-based indexes,
            # use user_ind_expressions.column_expression instead of
            # user_ind_columns.column_name.
            index_columns = list()
            for column_pos, column_name, descend, column_expr in ind_col_rows:
                if column_expr is None or column_expr == '':
                    index_columns.append(column_name + ' ' + descend)
                else:
                    index_columns.append(column_expr + ' ' + descend)
            index_columns = '(' + ', '.join(index_columns) + ')'
            # Add index_columns to end of each index/row (index is a tuple!).
            indexes_rows[count] = index_row + (index_columns,)

        # Print output.
        print('\nHere are the indexes on table {}:'.format(my_table))
        writer1.write_rows(indexes_rows, indexes_col_names)
        writer1.close_output_file()
        return
    # End of method database_table_schema.

    def database_view_schema(self, colsep='|') -> None:
        """ Print the schema for a view.

        Parameters:
            colsep (str): column separator to use.
        Returns:
        """
        # The query for finding the views in this user's schema.
        # find_views_sql has no parameters.
        sql_x = mq.find_views_sql[self.db_type]
        if skip_operation(sql_x):
            if sql_x == mq.not_implemented:
                z = mq.not_implemented
                print('\n' + z.format("VIEWS", self.db_type.upper()))
            elif sql_x == mq.not_possible_sql:
                z = mq.not_possible_sql
                print(z.format(self.db_type.upper(), self.db_lib_name.upper()))
            return

        # Find views
        self.set_sql(sql_x)
        # TODO need to use all fields and make return values consistent.
        # views_col_names: view_name, view_sql, check_option, is_updatable,
        #                  is_insertable, is_deletable
        views_col_names, views, views_row_count = self.run_sql()

        # Create mapping from column name to index, so I can access items by
        # column name instead of by index.
        columns = {name.lower(): index for
                   index, name in enumerate(views_col_names)}

        if len(views) == 0:
            print('You own no views!  Nothing to see!')
            return
        elif len(views) == 1:
            # Only one view.  Choose it.
            my_view = views[0]
            z = '\nYou have only one view: {}.'
            print(z.format(my_view[columns['view_name']]))
        else:
            # Ask end-user to choose a view.
            prompt = '\nHere are the views available to you:\n'
            for index, (view_name, view_sql) in enumerate(views):
                prompt += str(index + 1) + ': ' + view_name + '\n'
            prompt += ('Enter the number for the view you want info about,\n'
                       'Or enter "Q" to quit:\n')

            # Keep looping until valid choice made.
            while True:
                choice = input(prompt).strip().upper()
                # Interpret the choice.
                if choice == "Q":
                    print('Quitting as requested.')
                    exit(0)
                choice = int(choice)
                if choice in range(len(views)):
                    my_view = views[choice - 1]
                    break
                else:
                    print('Invalid choice, please try again.')

        # Unpack information.
        my_view_name = my_view[columns['view_name']]
        my_view_sql = my_view[columns['view_sql']]

        # Print the sql for this view.
        z = '\nHere is the SQL for view {}:\n"{}"'
        print(z.format(my_view_name, my_view_sql))

        # Set up to write output.
        writer1 = OutputWriter(out_file_name='', align_col=True, col_sep=colsep)

        # Find and print columns in this view.
        sql_x = mq.find_view_col_sql[self.db_type]
        if skip_operation(sql_x):
            if sql_x == mq.not_implemented:
                z = mq.not_implemented
                print(z.format("VIEW'S COLUMNS", self.db_type.upper()))
            elif sql_x == mq.not_possible_sql:
                z = mq.not_possible_sql
                print(z.format(self.db_type.upper(), self.db_lib_name.upper()))
            return
        print('\nHere are the columns for view {}:'.format(my_view_name))
        columns_col_names, columns_rows = self._find_view_columns(my_view_name)

        writer1.write_rows(columns_rows, columns_col_names)
        writer1.close_output_file()

        return
    # End of method database_view_schema.

    def _find_table_columns(self, table_name: str) -> (list, list):
        """ Find the columns in a table.

        Parameters:
            table_name (str): the table to find the columns of.
        Returns:
            col_names (list): column names:
                [column_id, column_name, data_type, nullable, default_value,
                 comments]
            rows (list): list of tuples, each tuple holds info about one column,
                with the column names listed in col_names.
        """
        # The SQL to find the table columns, their order, and their data types.
        sql_x = mq.find_tab_col_sql[self.db_type]
        if sql_x.find('{}') > -1:
            sql_x = sql_x.replace('{}', table_name)

        # Execute the SQL.
        self.set_sql(sql_x)
        columns_col_names, columns_rows, columns_row_count = self.run_sql()

        # Replace None by '(null)' everywhere.
        columns_rows = [[no_none(x, '(null)') for x in r] for r in columns_rows]

        # Return the column information.
        return columns_col_names, columns_rows
    # End of method _find_table_columns.

    def _find_view_columns(self, view_name: str) -> (list, list):
        """ Find the columns in a view.

        Parameters:
            view_name (str): the table to find the columns of.
        Returns:
            col_names (list): column names:
                [column_id, column_name, data_type, nullable, default_value,
                 comments]
            rows (list): list of tuples, each tuple holds info about one column,
                with the column names listed in col_names.
        """
        # The SQL to find the view columns, their order, and their data types.
        sql_x = mq.find_view_col_sql[self.db_type]
        if sql_x.find('{}') > -1:
            sql_x = sql_x.replace('{}', view_name)

        self.set_sql(sql_x)
        columns_col_names, columns_rows, columns_row_count = self.run_sql()

        # Replace None by '(null)' everywhere.
        columns_rows = [[no_none(x, '(null)') for x in r] for r in columns_rows]

        # Return the column information.
        return columns_col_names, columns_rows
    # End of method _find_view_columns.

    def _find_indexes(self, table_name: str) -> (list, list):
        """ Find the indexes in a table.

        Parameters:
            table_name (str): the table to find the indexes of.
        Returns:
            col_names (list): column names:
                [index_name, index_type, table_type, unique]
            rows (list): list of tuples, each tuple holds info about one index:
                tuple = (index_name, index_type, table_type, unique).
        """
        # The SQL to find the indexes for this table.
        sql_x = mq.find_indexes_sql[self.db_type]
        if sql_x.find('{}') > -1:
            sql_x = sql_x.replace('{}', table_name)

        # Execute the SQL.
        self.set_sql(sql_x)
        indexes_col_names, indexes_rows, indexes_row_count = self.run_sql()

        # Return the index information.
        return indexes_col_names, indexes_rows
    # End of method _find_indexes.

    def _find_index_columns(self, index_name: str) -> (list, list):
        """ Find the columns in an index.

        Parameters:
            index_name (str): the index to find the columns in.
        Returns:
            col_names (list): column names:
                [column_position, column_name, descend, column_expression]
            rows (list): list of tuples, a tuple has info about 1 index column:
              tuple = (column_position, column_expression/column_name, descend).
        """
        # The SQL to find the columns for this index.
        sql_x = mq.find_ind_col_sql[self.db_type]
        if sql_x.find('{}') > -1:
            sql_x = sql_x.replace('{}', index_name)

        # Execute the SQL.
        self.set_sql(sql_x)
        ind_col_col_names, ind_col_rows, ind_col_row_count = self.run_sql()

        # Return the information about this index.
        return ind_col_col_names, ind_col_rows
    # End of method _find_index_columns.
# End of Class DBClient.
