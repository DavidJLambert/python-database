""" DBClient.py """
from DBInstance import *
from OutputWriter import *


class DBClient(object):
    """ Get text of a SQL program with bind variables, then execute it.

    Attributes:
        sql (str): the text of a SQL program with bind variables.
        bind_vars (dict): the bind variables' names and values.
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
        self.db_library_name = self.db_instance.get_db_lib_name()
        db_library = __import__(self.db_library_name)
        self.db_library = db_library

        # Get database cursor.
        self.cursor = self.db_instance.create_cursor(self)

        # Placeholders for SQL text and bind variables.
        # To avoid PyCharm warnings about variables defined outside of __init__.
        self.sql: str = ''
        self.bind_vars = dict()
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

    def set_sql(self, sql: str, bind_vars: dict) -> None:
        """ Set text of SQL to execute + values of any bind variables in it.

        Parameters:
            sql (str): text of the SQL to execute.
            bind_vars (dict): the bind variables' names and values.
        Returns:
        """
        self.sql: str = sql
        self.bind_vars: dict = bind_vars
        return
    # End of method set_sql.

    def set_sql_at_prompt(self) -> None:
        """ Set text of SQL at a prompt.

        Parameters:
        Returns:
        """
        # Get text of SQL program.
        prompt = ('\nEnter the lines of a SQL program.'
                  '\nHit Return to stop entering the SQL.'
                  '\nAt any point, enter "Q" to Quit this program:\n')
        sql = ''
        while True:
            # Get the next line of SQL.
            response = input(prompt).strip()
            # Use a prompt only for the first line of SQL.
            prompt = ''

            if response == '':
                # Done entering SQL.
                break
            elif response.upper() == 'Q':
                print('\nQuitting at your request.')
                self.clean_up()
                exit(0)
            else:
                # Add more text to the current SQL.
                sql += '\n' + response
        self.sql = sql.strip()
        return
    # End of method set_sql_at_prompt.

    def set_bind_vars_at_prompt(self) -> None:
        """ Set bind variables at a prompt.

        Parameters:
        Returns:
        """
        from datetime import datetime
        # Prompts for calling input().
        prompt_name = ("\nEnter a bind variable name (omit the colon)."
                       "\nOr hit Return when done entering bind variables:\n")
        prompt_type = ("\nEnter the letter of the bind variable's data type:"
                       "\n(T)ext."
                       "\n(I)nteger."
                       "\n(R)eal (aka decimal)."
                       "\n(D)ate.\n")
        prompt_value = "\nEnter the bind variable value.\n"
        prompt_text = "Do not put quotes around text, that's done for you.\n"
        prompt_date = "Use the date format 'm/d/yyyy'.\n"

        # Dict with keys = bind variable names, values = bind variable values.
        bind_vars = dict()
        # One loop per bind variable name/value pair.
        while True:
            # Get bind variable name.
            bindvar_name = input(prompt_name).strip()
            if bindvar_name == '':
                # Done entering SQL.
                break
            elif bindvar_name.upper() == 'Q':
                print('\nQuitting at your request.')
                self.clean_up()
                exit(0)

            # Get bind variable data type.
            while True:
                bindvar_type = input(prompt_type).strip().upper()
                if len(bindvar_type) != 1:
                    print('Enter one character only, please try again.')
                elif bindvar_type not in 'TIRD':
                    print('Invalid entry, please try again.')
                else:
                    # Valid input.
                    break

            # Get bind variable value.
            bindvar_val = None
            if bindvar_type == 'T':
                bindvar_val = input(prompt_value + prompt_text).strip()
            elif bindvar_type == 'I':
                bindvar_val = int(input(prompt_value).strip())
            elif bindvar_type == 'R':
                bindvar_val = float(input(prompt_value).strip())
            elif bindvar_type == 'D':
                while True:
                    bindvar_val = input(prompt_value + prompt_date).strip()
                    try:
                        bindvar_val = datetime.strptime(bindvar_val, '%m/%d/%Y')
                        break
                    except ValueError:
                        print('Invalid date, please try again.')
            else:
                print('\nProgram bug.')
                self.clean_up()
                exit(0)

            # Add bind variable name/value pair to dictionary of bind variables.
            bind_vars[bindvar_name] = bindvar_val

        # All done!
        self.bind_vars = bind_vars
        return
    # End of method set_bind_vars_at_prompt.

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
                elif not sql_type == 'SELECT':
                    # Not a CRUD statement.
                    pass
                elif sql_type == 'SELECT':
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

    def database_table_schema(self, colsep='|') -> None:
        """ Print the schema for a table.

        Parameters:
            colsep (str): column separator to use.
        Returns:
        """
        # Find tables
        if self.db_type == access:
            z = 'SQL CANNOT READ THE SCHEMA IN MS ACCESS THROUGH {}.'
            print(z.format(lib_name_for_db[access].upper()))
            return
        tables = self._find_tables()

        if len(tables) == 0:
            print('You own no tables!  Nothing to see!')
            return
        elif len(tables) == 1:
            # Only one table.  Choose that table to show.
            my_table = tables[0]
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

        # Set up to write output.
        writer1 = OutputWriter(out_file_name='', align_col=True, col_sep=colsep)

        # Find and print columns in this table.
        columns_col_names, columns_rows = self._find_table_columns(my_table)
        writer1.write_rows(columns_rows, columns_col_names)

        # Find all indexes in this table.
        indexes_col_names, indexes_rows = self._find_indexes(my_table)

        # Add 'INDEX_COLUMNS' to end of col_names.
        indexes_col_names.append('INDEX_COLUMNS')

        # Go through indexes, add index_columns to end of each index/row.
        for count, index_row in enumerate(indexes_rows):
            index_name = index_row[0]
            _, ind_col_rows = self._find_index_columns(index_name)
            # Concatenate names of columns in index.  In function-based indexes,
            # use user_ind_expressions.column_expression instead of
            # user_ind_columns.column_name.
            index_columns = ''
            for column_pos, column_name, descend, column_expr in ind_col_rows:
                if index_columns != '':
                    index_columns += ', '
                if column_expr is None:
                    index_columns += column_name + ' ' + descend
                else:
                    index_columns += column_expr + ' ' + descend
            index_columns = '(' + index_columns + ')'
            # Add index_columns to end of each index/row (index is a tuple!).
            indexes_rows[count] = index_row + (index_columns,)

        # Print output.
        print()
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
        if self.db_type == access:
            z = 'SQL CANNOT READ THE SCHEMA IN MS ACCESS THROUGH {}.'
            print(z.format(lib_name_for_db[access].upper()))
            return
        views_col_names, views = self._find_views()

        # Create mapping from column name to index, so I can access items by
        # column name instead of by index.
        columns = {name: index for index, name in enumerate(views_col_names)}

        if len(views) == 0:
            print('You own no views!  Nothing to see!')
            return
        elif len(views) == 1:
            # Only one view.  Choose it.
            choice = 0
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
                    choice -= 1
                    break
                else:
                    print('Invalid choice, please try again.')

        # Unpack information.
        my_view_name = views[choice][columns['VIEW_NAME']]
        my_view_sql = views[choice][columns['VIEW_SQL']]

        # Print the sql for this view.
        print('\nHere is the SQL for this view:\n"{}"'.format(my_view_sql))

        # Set up to write output.
        writer1 = OutputWriter(out_file_name='', align_col=True, col_sep=colsep)

        # Find and print columns in this view.
        columns_col_names, columns_rows = self._find_view_columns(my_view_name)
        writer1.write_rows(columns_rows, columns_col_names)
        writer1.close_output_file()
        return
    # End of method database_view_schema.

    # EVERYTHING ABOVE IS DATABASE TYPE INDEPENDENT, NOT BELOW.

    def _find_tables(self) -> list:
        """ Find the tables in this user's schema.

        Parameters:
        Returns:
            table_names (list): list of the tables in this user's schema.
        """
        # The query for finding the tables in this user's schema.
        sql_x = ''
        bind_vars_x = dict()
        if self.db_type == oracle:
            sql_x = "SELECT table_name FROM user_tables ORDER BY table_name"
        elif self.db_type == access:
            pass
        elif self.db_type == sqlite:
            sql_x = ("SELECT name FROM sqlite_master WHERE type='table' "
                     "AND name NOT LIKE 'sqlite_%' ORDER BY name")
        elif self.db_type == sql_server:
            sql_x = "SELECT name FROM sys.tables WHERE type='U'"

        # Execute the SQL.
        self.set_sql(sql_x, bind_vars_x)
        tables_col_names, tables_rows, tables_row_count = self.run_sql()

        # Return the list of table names.
        return [row[0] for row in tables_rows]
    # End of method _find_tables.

    def _find_views(self) -> (list, list):
        """ Find the views in this user's schema.

        Parameters:
        Returns:
            views_col_names (list): column names: [view_name, view_sql]
            view_names (list): list of tuples, each tuple being the columns
                above for the views in this user's schema.
        """
        # The query for finding the views in this user's schema.
        sql_x = ''
        bind_vars_x = dict()
        if self.db_type == oracle:
            sql_x = ("SELECT view_name, text AS view_sql "
                     "FROM user_views ORDER BY view_name")
        elif self.db_type == access:
            pass
        elif self.db_type == sqlite:
            sql_x = ("SELECT name FROM sqlite_master WHERE type='view' "
                     "ORDER BY name")
        elif self.db_type == sql_server:
            sql_x = "SELECT name FROM sys.views WHERE type='V'"

        # Execute the SQL.
        self.set_sql(sql_x, bind_vars_x)
        views_col_names, views_rows, views_row_count = self.run_sql()

        # Return the list of view names.
        return views_col_names, views_rows
    # End of method _find_views.

    def _find_table_columns(self, table_name: str) -> (list, list):
        """ Find the columns in a table.

        Parameters:
            table_name (str): the table to find the columns of.
        Returns:
            col_names (list): column names:
                [column_id, column_name, data_type, nullable, data_default,
                 comments]
            rows (list): list of tuples, each tuple holds info about one column,
                with the column names listed in col_names.
        """
        # The SQL to find the columns, their order, and their data types.
        sql_x = ''
        bind_vars_x = dict()
        if self.db_type == oracle:
            sql_x = """
            SELECT column_id, c.column_name,
              case
                when (data_type LIKE '%CHAR%' OR data_type IN ('RAW','UROWID'))
                  then data_type||'('||c.char_length||
                       decode(char_used,'B',' BYTE','C',' CHAR',null)||')'
                when data_type = 'NUMBER'
                  then
                    case
                      when c.data_precision is null and c.data_scale is null
                        then 'NUMBER'
                      when c.data_precision is null and c.data_scale is not null
                        then 'NUMBER(38,'||c.data_scale||')'
                      else data_type||'('||c.data_precision||','||c.data_scale||')'
                      end
                when data_type = 'BFILE'
                  then 'BINARY FILE LOB (BFILE)'
                when data_type = 'FLOAT'
                  then data_type||'('||to_char(data_precision)||')'||
                       decode(data_precision, 126,' (double precision)',
                       63,' (real)',null)
                else data_type
                end data_type,
                decode(nullable,'Y','Yes','No') nullable,
                data_default,
                NVL(comments,'(null)') comments
            FROM user_tab_cols c, user_col_comments com
            WHERE c.table_name = :table_name
            AND c.table_name = com.table_name
            AND c.column_name = com.column_name
            ORDER BY column_id"""
            bind_vars_x = {"table_name": table_name}
        elif self.db_type == access:
            pass
        elif self.db_type == sqlite:
            # TODO pragma not working
            sql_x = "PRAGMA table_info({})".format(table_name)
        elif self.db_type == sql_server:
            # TODO not the correct SQL
            sql_x = ("SELECT order, name, datatype, nullable, comment "
                     "FROM sys.columns c, sys.tables t "
                     "WHERE type='V'")
            '''
            SELECT c.column_id, c.Name AS Field_Name, t.Name AS Data_Type, t.max_length AS Length_Size, t.precision AS Precision, is_nullable
            FROM sys.columns c 
            INNER JOIN sys.objects o ON o.object_id = c.object_id 
            LEFT JOIN sys.types t on t.user_type_id  = c.user_type_id 
            WHERE o.type = 'U' 
            and o.Name = 'YourTableName' 
            ORDER BY c.column_id, c.Name
            '''
        # Execute the SQL.
        self.set_sql(sql_x, bind_vars_x)
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
                [column_id, column_name, data_type, nullable, data_default,
                 comments]
            rows (list): list of tuples, each tuple holds info about one column,
                with the column names listed in col_names.
        """
        sql_x = ''
        if self.db_type == oracle:
            return self._find_table_columns(view_name)
        elif self.db_type == access:
            pass
        elif self.db_type == sqlite:
            sql_x = ("SELECT name FROM sqlite_master WHERE type='view' "
                     "ORDER BY name")

        bind_vars_x = dict()
        self.set_sql(sql_x, bind_vars_x)
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
                [index_name, index_type, table_type, uniqueness]
            rows (list): list of tuples, each tuple holds info about one index:
                tuple = (index_name, index_type, table_type, uniqueness).
        """
        # The query for finding the index in this table.
        sql_x = ''
        bind_vars_x = dict()
        if self.db_type == oracle:
            sql_x = ("SELECT index_name, index_type, table_type, uniqueness "
                     "FROM user_indexes WHERE table_name = :table_name "
                     "ORDER BY index_name")
            bind_vars_x = {"table_name": table_name}
        elif self.db_type == access:
            pass
        elif self.db_type == sqlite:
            # TODO pragma not working
            sql_x = "PRAGMA index_list({})".format(table_name)

        # Execute the SQL.
        self.set_sql(sql_x, bind_vars_x)
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
        # The query for finding the columns in this index.
        sql_x = ''
        bind_vars_x = dict()
        if self.db_type == oracle:
            sql_x = ("SELECT ic.column_position, column_name, descend, "
                     "column_expression FROM user_ind_columns ic "
                     "LEFT OUTER JOIN user_ind_expressions ie "
                     "ON ic.column_position = ie.column_position "
                     "AND ic.index_name = ie.index_name "
                     "WHERE ic.index_name = :index_name "
                     "ORDER BY ic.column_position")
            bind_vars_x = {"index_name": index_name}
        elif self.db_type == access:
            pass
        elif self.db_type == sqlite:
            # TODO pragma not working
            sql_x = "PRAGMA index_info({})".format(index_name)

        # Execute the SQL.
        self.set_sql(sql_x, bind_vars_x)
        ind_col_col_names, ind_col_rows, ind_col_row_count = self.run_sql()

        # Return the information about this index.
        return ind_col_col_names, ind_col_rows
    # End of method _find_index_columns.
# End of Class DBClient.
