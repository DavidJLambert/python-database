""" DBClient.py

REPOSITORY: https://github.com/DavidJLambert/Python-Universal-DB-Client

AUTHOR: David J. Lambert

VERSION: 0.7.4

DATE: Apr 19, 2020
"""
from OutputWriter import *
import MyQueries as mq
from DBInstance import *


# noinspection PyUnresolvedReferences
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
        return
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
                all_rows: list of tuples, each tuple is one row being fetched.
                row_count: number of rows fetched.
            For other types of SQL:
                list()
                list()
                row_count: number of rows affected.
        """
        col_names = list()
        all_rows = list()
        row_count = 0
        if not self.sql:
            print('NO SQL TO EXECUTE.')
            self.clean_up()
            exit(1)
        else:
            try:
                # Execute SQL.
                if len(self.bind_vars) > 0:
                    if self.db_type == access:
                        print('NO BIND VARIABLES ALLOWED IN MICROSOFT ACCESS.')
                        self.clean_up()
                        exit(1)
                    self.cursor.execute(self.sql, self.bind_vars)
                else:
                    self.cursor.execute(self.sql)

                # Check for something really yucky.
                if self.cursor is None:
                    print('\nCursor is None.')
                    self.clean_up()
                    exit(1)

                # Classify SQL.
                sql_type: str = self.sql.split()[0].upper()

                row_count = self.cursor.rowcount

                # Handle SQL results.
                if sql_type in {'INSERT', 'UPDATE', 'DELETE'}:
                    self.cursor.commit()
                elif sql_type != 'SELECT':
                    print('Not a CRUD statement!')
                elif sql_type == 'SELECT':
                    # Fetch rows.  Fetchall for large number of rows a problem.
                    all_rows = self.cursor.fetchall()

                    # Get column names
                    col_names = [item[0] for item in self.cursor.description]

                    # Column data types.  Too specific and inconsistent to use.
                    # col_types = [item[1] for item in self.cursor.description]

                    # Could combine all_rows & col_names into dict, with keys
                    # from col_names & values from all_rows, but performance
                    # would suffer.

                    # In Oracle, cursor.rowcount = 0, so get row count directly.
                    row_count = len(all_rows)

            except self.db_library.Error:
                print_stacktrace()
            finally:
                return col_names, all_rows, row_count
    # End of method run_sql.

    def _skip_op_msg(self, sql_x: str, object_str: str) -> str:
        """ Method to print message if skipping operation in db_table_schema or
            db_view_schema.

        Parameters:
            sql_x (str): most of message to print.
            object_str (str): type of object where we're skipping looking at in
                              data dictionary.
        Returns:
            msg (str): complete message to print.
        """
        if sql_x == mq.not_implemented:
            msg = '\n' + sql_x.format(object_str, self.db_type.upper())
        elif sql_x == mq.not_possible_sql:
            msg = sql_x.format(self.db_type.upper(), self.db_lib_name.upper())
        else:
            msg = "Problem in _skip_op_msg!"
            print(msg)
            exit(1)
        return msg
    # End of method _skip_op_msg.

    # THE REST OF THE METHODS ALL HAVE TO DO WITH SEEING THE DATABASE SCHEMA.

    # EVERYTHING ABOVE IS DB_TYPE INDEPENDENT.  BELOW, NOT INDEPENDENT.

    def db_table_schema(self, colsep='|') -> None:
        """ Print the schema for a table.

        Parameters:
            colsep (str): column separator to use.
        Returns:
        """
        # Find tables.
        skip_op, table_col_names, table_rows = self._data_dict_fetch(mq.tables,
                                                                     '')
        if skip_op:
            return

        # Create map from column name to item #, so items accessible by column
        # name instead of by item #.
        columns = {name.lower(): item_num for
                   item_num, name in enumerate(table_col_names)}

        # Extract a list of table names.
        table_names = [table[columns['table_name']] for table in table_rows]

        # Choose a table.
        choice = pick_one(table_names, "table")
        if choice == -1:
            # No tables.
            return

        # Unpack information.
        my_table_name = table_names[choice]

        # Find and print columns in this table.
        skip_op, columns_col_names, columns_rows = self._data_dict_fetch(
            mq.tab_col, my_table_name)

        # Write output.
        writer1 = OutputWriter(out_file_name='', align_col=True, col_sep=colsep)
        writer1.write_rows(columns_rows, columns_col_names)
        print()

        # Find all indexes in this table.
        skip_op, indexes_col_names, indexes_rows = self._data_dict_fetch(
            mq.indexes, my_table_name)
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
            skip_op, _, ind_col_rows = self._data_dict_fetch(mq.ind_col,
                                                             index_name)
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
    # End of method db_table_schema.

    def db_view_schema(self, colsep='|') -> None:
        """ Print the schema for a view.

        Parameters:
            colsep (str): column separator to use.
        Returns:
        """
        # Find views
        # TODO need to use all fields and make return values consistent.
        skip_op, view_col_names, view_rows = self._data_dict_fetch(mq.views, '')
        if skip_op:
            return

        # Create mapping from column name to item #, so I can access items by
        # column name instead of by item #.
        columns = {name.lower(): item_num for
                   item_num, name in enumerate(view_col_names)}

        # Extract a list of view names.
        view_names = [view[columns['view_name']] for view in view_rows]

        # Choose a view.
        choice = pick_one(view_names, "view")
        if choice == -1:
            # No views.
            return

        # Unpack information.
        my_view_name = view_names[choice]
        my_view_sql = view_rows[choice][columns['view_sql']]

        # Print the sql for this view.
        z = '\nHere is the SQL for view {}:\n"{}"'
        print(z.format(my_view_name, my_view_sql))

        # Find and print columns in this view.
        print('\nHere are the columns for view {}:'.format(my_view_name))
        skip_op, columns_col_names, columns_rows = self._data_dict_fetch(
            mq.view_col, my_view_name)

        # Write output.
        writer1 = OutputWriter(out_file_name='', align_col=True, col_sep=colsep)
        writer1.write_rows(columns_rows, columns_col_names)
        writer1.close_output_file()

        return
    # End of method db_view_schema.

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
            column_names (list): the names of the columns in "rows"
            rows (list): list of tuples, a tuple has info about 1 object
        """
        # The SQL to find the columns for this object.
        sql = mq.data_dict_sql[obj_type, self.db_type]

        if is_skip_operation(sql):
            print(self._skip_op_msg(sql, obj_type))
            return True, list(), list()
        else:
            if sql.find('{}') > -1:
                sql = sql.replace('{}', obj_name)

            # Execute the SQL.
            self.set_sql(sql)
            column_names, rows, row_count = self.run_sql()
            # Return the information about this object.
            return False, column_names, rows
    # End of method _data_dict_fetch.

    def get_data_type(self, table: str, column: str) -> (str, str):
        """ Find the data type of table.column.
            Only used in UniversalClient_Complex.py.

        Parameters:
            table (str): the table to which the column belongs.
            column (str): the column we want the data type of.
        Returns:
            data_type (str): the data type of table_name.column_name, as
                described in the database's data dictionary.
            data_type_group (str): the data type's group.
        """
        # Get the columns for this table.
        skip_op, columns_col_names, columns_rows = self._data_dict_fetch(
            mq.tab_col, table)
        if skip_op:
            return 'NOT FOUND', 'NOT FOUND'

        # Create mapping from column name to item #.
        columns = {name.lower(): item_num for
                   item_num, name in enumerate(columns_col_names)}
        data_type = 'NOT FOUND'
        for columns_row in columns_rows:
            if columns_row[columns['column_name']].upper() == column.upper():
                data_type = columns_row[columns['data_type']]
                break

        data_type_low = data_type.lower()
        # Now find data type group.
        if data_type == 'NOT FOUND':
            data_type_group = data_type
        elif ((data_type_low.find('nchar') > -1) or
              (data_type_low.find('nvarchar') > -1) or
              (data_type_low.find('nclob') > -1)):
            data_type_group = 'UNICODE'
        elif ((data_type_low.find('bit') > -1 and self.db_type != sqlserver) or
              (data_type_low.find('raw') > -1) or
              (data_type_low.find('image') > -1) or
              (data_type_low.find('binary') > -1) or
              (data_type_low.find('blob') > -1) or
              (data_type_low.find('byte') > -1 and self.db_type != oracle)):
            # Includes Oracle Long Raw, listed before Long.
            # Image is Sql server
            data_type_group = 'BINARY'
        elif ((data_type_low.find('char') > -1) or
              (data_type_low.find('text') > -1) or
              (data_type_low.find('long') > -1) or
              (data_type_low.find('clob') > -1)):
            data_type_group = 'STRING'
        elif ((data_type_low.find('int') > -1) or
              (data_type_low.find('number') > -1) or
              (data_type_low.find('numeric') > -1) or
              (data_type_low.find('decimal') > -1) or
              (data_type_low.find('real') > -1) or
              (data_type_low.find('float') > -1) or
              (data_type_low.find('double') > -1) or
              (data_type_low.find('money') > -1) or
              (data_type_low.find('currency') > -1)):
            data_type_group = 'NUMBER'
        elif ((data_type_low.find('date') > -1) or
              (data_type_low.find('time') > -1) or
              (data_type_low.find('interval') > -1) or
              (data_type_low.find('year') > -1)):
            data_type_group = 'DATETIME'
        elif ((data_type_low.find('bool') > -1) or
              (data_type_low.find('bit') > -1 and self.db_type == sqlserver)):
            data_type_group = 'BOOLEAN'
        else:
            data_type_group = 'OTHER'

        return data_type, data_type_group
    # End of method get_data_type.

# End of Class DBClient.
