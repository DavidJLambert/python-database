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
                elif sql_type not in {'SELECT'}:
                    # Not a CRUD statement.
                    pass
                elif sql_type in {'SELECT'}:
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

    def _print_skip_op(self, sql_x: str, object_str: str) -> None:
        """ Method to print message if skipping operation in
            database_table_schema or database_view_schema.

        Parameters:
            sql_x (str): most of message to print.
            object_str (str): type of object where we're skipping looking at in
                              data dictionary.
        Returns:
        """
        if sql_x == mq.not_implemented:
            print('\n' + sql_x.format(object_str, self.db_type.upper()))
        elif sql_x == mq.not_possible_sql:
            print(sql_x.format(self.db_type.upper(), self.db_lib_name.upper()))
        return
    # End of function _print_skip_op.

    def _pick_one(self, choices: list, choice_type: str) -> int:
        """ Method to print message if skipping operation in
            database_table_schema or database_view_schema.

        Parameters:
            choices (list): list of choices (string) to pick one from.
            choice_type (str): the name of the type being chosen.
        Returns:
            choice (int): the choice made.
        """
        if len(choices) == 0:
            print('You own no tables!  Nothing to see!')
            return 0
        elif len(choices) == 1:
            # Only one table.  Choose it.
            choice_name = choices[0]
            print('\nYou have one {}: {}.'.format(choice_type, choice_name))
        else:
            # Ask end-user to choose a table.
            prompt = '\nHere are the {} available to you:\n'.format(choice_type)
            for item_num, choice_name in enumerate(choices):
                prompt += str(item_num + 1) + ': ' + choice_name + '\n'
            prompt += ('Enter the number for the {} you want info about,\n'
                       'Or enter "Q" to quit:\n'.format(choice_type))

            # Keep looping until valid choice made.
            while True:
                choice = input(prompt).strip().upper()
                # Interpret the choice.
                if choice == "Q":
                    print('Quitting as requested.')
                    exit(0)
                choice = int(choice) - 1
                if choice in range(len(choices)):
                    choice_name = choices[choice]
                    break
                else:
                    print('Invalid choice, please try again.')
            print('\nYou chose {} "{}":.'.format(choice_type, choice_name))

        return choice
    # End of function _pick_one.

    # THE REST OF THE METHODS ALL HAVE TO DO WITH SEEING THE DATABASE SCHEMA.

    # EVERYTHING ABOVE IS DB_TYPE INDEPENDENT.  BELOW, NOT INDEPENDENT.

    def database_table_schema(self, colsep='|') -> None:
        """ Print the schema for a table.

        Parameters:
            colsep (str): column separator to use.
        Returns:
        """
        # Find tables.
        skip_op, table_col_names, table_rows =\
            self._data_dict_fetch(mq.tables, '')
        if skip_op:
            return

        # Create map from column name to item #, so items accessible by column
        # name instead of by item #.
        columns = {name.lower(): item_num for
                   item_num, name in enumerate(table_col_names)}

        # Extract a list of table names.
        table_names = [table[columns['table_name']] for table in table_rows]

        # Choose a table.
        choice = self._pick_one(table_names, "table")

        # Unpack information.
        my_table_name = table_names[choice]

        # Find and print columns in this table.
        skip_op, columns_col_names, columns_rows =\
            self._data_dict_fetch(mq.tab_col, my_table_name)

        # Write output.
        writer1 = OutputWriter(out_file_name='', align_col=True, col_sep=colsep)
        writer1.write_rows(columns_rows, columns_col_names)
        print()

        # Find all indexes in this table.
        skip_op, indexes_col_names, indexes_rows =\
            self._data_dict_fetch(mq.indexes, my_table_name)
        if skip_op:
            return

        # Add 'INDEX_COLUMNS' to end of col_names.
        indexes_col_names.append('INDEX_COLUMNS')

        # Create map from column name to item #, so items accessible by
        # column name instead of by item #.
        columns = {name.lower(): item_num for
                   item_num, name in enumerate(indexes_col_names)}

        # Go through indexes, add index_columns to end of each index/row.
        for item_num, index_row in enumerate(indexes_rows):
            index_name = index_row[columns['index_name']]
            skip_op, _, ind_col_rows =\
                self._data_dict_fetch(mq.ind_col, index_name)
            if skip_op:
                return
            # Concatenate names of columns in index.
            index_columns = list()
            for column_pos, column_name, descend, column_expr in ind_col_rows:
                if column_expr is None or column_expr == '':
                    index_columns.append(column_name + ' ' + descend)
                else:
                    index_columns.append(column_expr + ' ' + descend)
            index_columns = '(' + ', '.join(index_columns) + ')'
            # Add index_columns to end of each index/row (index is a tuple!).
            indexes_rows[item_num] = index_row + (index_columns,)

        # Print output.
        print('\nHere are the indexes on table {}:'.format(my_table_name))
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
        # Find views
        # TODO need to use all fields and make return values consistent.
        skip_op, view_col_names, view_rows = self._data_dict_fetch(mq.views, '')

        # Create mapping from column name to item #, so I can access items by
        # column name instead of by item #.
        columns = {name.lower(): item_num for
                   item_num, name in enumerate(view_col_names)}

        # Extract a list of view names.
        view_names = [view[columns['view_name']] for view in view_rows]

        # Choose a view.
        choice = self._pick_one(view_names, "view")

        # Unpack information.
        my_view_name = view_names[choice]
        my_view_sql = view_rows[choice][columns['view_sql']]

        # Print the sql for this view.
        z = '\nHere is the SQL for view {}:\n"{}"'
        print(z.format(my_view_name, my_view_sql))

        # Find and print columns in this view.
        print('\nHere are the columns for view {}:'.format(my_view_name))
        skip_op, columns_col_names, columns_rows =\
            self._data_dict_fetch(mq.view_col, my_view_name)

        # Write output.
        writer1 = OutputWriter(out_file_name='', align_col=True, col_sep=colsep)
        writer1.write_rows(columns_rows, columns_col_names)
        writer1.close_output_file()

        return
    # End of method database_view_schema.

    def _data_dict_fetch(self, obj_type: str, obj_name: str) -> (bool, list,
                                                                 list):
        """ Find data dictionary information about a type of object.

        Parameters:
            obj_type (str): the type of object to collect info about (tables,
                table columns, views, view columns, indexes, or index columns).
            obj_name (str): the name of the object to collect info about (table
                for table columns, view for view columns, table for indexes, or
                index for index columns).
        Returns:
            skip_op (bool): whether or this operation was skipped.
            column_names (list): the names of the columns in "rows".
            rows (list): list of tuples, a tuple has info about 1 object
        """
        # The SQL to find the columns for this index.
        sql_x = mq.data_dict_sql[obj_type, self.db_type]

        if is_skip_operation(sql_x):
            self._print_skip_op(sql_x, obj_type)
            return True, list(), list()
        else:
            if sql_x.find('{}') > -1:
                sql_x = sql_x.replace('{}', obj_name)

            # Execute the SQL.
            self.set_sql(sql_x)
            column_names, rows, row_count = self.run_sql()
            # Return the information about this object.
            return False, column_names, rows
    # End of method _data_dict_fetch.
# End of Class DBClient.
