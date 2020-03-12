""" OracleClient.py

SUMMARY:
  Class DBClient executes SQL with bind variables.

REPOSITORY: https://github.com/DavidJLambert/Python-Universal-DB-Client

AUTHOR: David J. Lambert

VERSION: 0.4.7

DATE: Mar 12, 2020

PURPOSE:
  A sample of my Python coding, to demonstrate that I can write decent Python,
  test it, and document it.  I also demonstrate I know relational databases.

DESCRIPTION:
  Class DBInstance encapsulates all the info needed to log into a database
  instance, plus it encapsulates the connection handle to that database.
  Its externally useful methods are:
  1)  print_all_connection_parameters: prints all the connection parameters.
  2)  close_connection: closes the connect to the database.
  3)  print_connection_status: whether or not DBInstance is connected to the db.

  Class DBClient executes SQL with bind variables, and then prints the results.
  Its externally useful methods are:
  1)  set_sql: gets SQL and bind variables as arguments.
  2)  set_sql_at_prompt: reads SQL from a prompt.
  3)  set_bind_vars_at_prompt: reads bind variable names & values from a prompt.
  4)  run_sql: executes SQL, which was read as a text variable (with set_sql)
      or entered at a prompt (by set_sql_at_prompt and set_bind_vars_at_prompt).
  5)  database_table_schema: lists all the tables owned by the current login,
      all the columns in those tables, and all indexes on those tables.
  6)  database_view_schema: lists all the views owned by the current login,
      all the columns in those views, and the SQL for the view.

  Class OutputWriter handles all query output to file or to standard output.
  Its externally useful methods are:
  1)  get_align_col: whether or not to align columns in output.
  2)  get_col_sep: get the character(s) to separate columns with.
  3)  get_out_file_name: get location to write output to (file or standard out).
  4)  write_rows: write output to location chosen in get_out_file_name.
  5)  olose_output_file: if writing to output file, close it.

  Stand-alone function run_sql_cmdline runs the database command line client
  (sqlplus, sqlcmd, etc.) as a subprocess.

  Stand-alone function ask_for_password(username) prompts for the password for
  the username provided as an argument.

  The code has been tested with CRUD statements (Create, Read, Update, Delete).
  There is nothing to prevent the end-user from entering other SQL, such as
  ALTER DATABASE, CREATE VIEW, and BEGIN TRANSACTION, but none have been tested.

  This program loads the entire result set into memory.  Thus, it is unsuitable
  for large results sets, which may not fit in the host's available RAM.

PROGRAM REQUIREMENTS:

  + For connecting to Oracle, my code uses the cx_Oracle library, available on
    PyPI.  The cx_Oracle library requires the Oracle client libraries.  Several
    ways to obtain the Oracle client libraries are documented on
    https://cx-oracle.readthedocs.io/en/latest/installation.html.

    Cx_Oracle v7.3.0 supports Python versions 2.7 and 3.5-3.8,
    and Oracle client versions 11.2-19.

SAMPLE DATABASES TO TEST THIS PROGRAM ON:
  The sample database has the small version of the Dell DVD Store database,
  version 2.1, available at http://linux.dell.com/dvdstore.  The data in these
  tables:

  - CATEGORIES     --     16 rows
  - CUSTOMERS      -- 20,000 rows
  - CUST_HIST      -- 60,350 rows
  - INVENTORY      -- 10,000 rows
  - ORDERLINES     -- 60,350 rows
  - ORDERS         -- 12,000 rows
  - PRODUCTS       -- 10,000 rows
  - REORDER        --      0 rows
  - I've added table db_description, has 1 row with my name and contact info.
"""

# -------- IMPORTS

from __future__ import print_function
import sys

# -------- CREATE AND INITIALIZE VARIABLES

ARRAY_SIZE = 20

# Supported database types.
oracle = 'oracle'
mysql = 'mysql'
sql_server = 'sql server'
postgresql = 'postgresql'
access = 'access'
sqlite = 'sqlite'
db_types = [oracle, mysql, sql_server, postgresql, access, sqlite]

uses_connection_string = set(db_types) - {mysql}

file_databases = {access, sqlite}

# -------- OS AND PYTHON VERSION STUFF

if True:
    # Limit scope of these imports to this block.
    from platform import uname, python_implementation
    from struct import calcsize

    q = ('OS: {}\nHost Name: {}\nOS Major Version: {}\nOS Full Version: {}'
         '\nProcessor Type: {}\nProcessor: {}')
    u = uname()
    q = q.format(u.system, u.node, u.release, u.version, u.machine, u.processor)
    print(q)

    sys_version_info = sys.version_info
    py_bits = 8 * calcsize("P")
    py_version = '.'.join(str(x) for x in sys_version_info)
    py_type = python_implementation()
    print('\n{} Version {}, {} bits.'.format(py_type, py_version, py_bits))


# -------- CUSTOM CLASSES


class DBInstance(object):
    """ Class containing database connection utilities and information
        about one database instance (referred to as "this database").

    Attributes:
        db_type (str): the type of database (Oracle, SQL Server, etc).
        db_path (str): the path of the database file, for SQLite and Access.
        username (str): a username for connecting to this database.
        password (str): the password for "username".
        hostname (str): the hostname of this database.
        port_num (int): the port this database listens on.
        instance (str): the name of this database instance.
        db_library (object): library object that was imported for this db_type.
        db_library_name (str): name of the library imported for this db_type.
        connection: the handle to this database. I set connection = None when
                    connection closed, this is not default behavior.
    """
    def __init__(self, db_type: str, db_path: str, username: str, password: str,
                 hostname: str, port_num: int, instance: str) -> None:
        """ Constructor method for this class.

        Parameters:
            db_type (str): the type of database (Oracle, SQL Server, etc).
            db_path (str): the path of the database file, for SQLite and Access.
            username (str): the username for the connection to this database.
            password (str): the password for "username".
            hostname (str): the hostname of this database.
            port_num (int): the port this database listens on.
            instance (str): the name of this database instance.
        Returns:
        """
        # Check if db_type valid.
        if db_type not in db_types:
            print('Invalid database type "{}".'.format(db_type))
            exit(1)

        # Library names for supported database types.
        db_libraries = {oracle: 'cx_Oracle', mysql: 'pymysql',
                        sql_server: 'pyodbc', postgresql: 'psycopg2',
                        access: 'pyodbc', sqlite: 'sqlite3'}

        # Import appropriate library.
        db_library = __import__(db_libraries[db_type])

        # Form database connection string.
        z = ''
        if db_type == mysql:
            z = ''
        elif db_type == sql_server:
            z = ('DRIVER={{SQL Server}};UID={};PWD={};SERVER={};PORT={};'
                 'DATABASE={}')
        elif db_type == oracle:
            z = '{}/{}@{}:{}/{}'
        elif db_type == postgresql:
            z = "user='{}' password='{}' host='{}' port='{}' dbname='{}'"
        elif db_type == access:
            z = 'DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={};'
        elif db_type == sqlite:
            z = '{}'
        else:
            print('Unknown db type {}, aborting.'.format(db_type))
            exit(1)

        if db_type in {sql_server, oracle, postgresql}:
            z = z.format(username, password, hostname, port_num, instance)
        elif db_type in file_databases:
            z = z.format(db_path)

        # Connect to database instance.
        self.connection = None
        try:
            if db_type in uses_connection_string:
                self.connection = db_library.connect(z)
            else:
                self.connection = db_library.\
                    connect(username, password, hostname, port_num, instance)
            print('Successfully connected to database.')
        except db_library.Error:
            print_stacktrace()
            print('Failed to connect to database.')
            exit(1)

        # Set database instance information.
        self.db_type: str = db_type
        self.db_path: str = db_path
        self.username: str = username
        self.password: str = password
        self.hostname: str = hostname
        self.port_num: int = port_num
        self.instance: str = instance
        self.db_library = db_library
        self.db_library_name = db_libraries[db_type]

        return

    # METHODS INVOLVING THE DATABASE CONNECTION.

    def close_connection(self) -> None:
        """ Method to close connection to this database.

        Parameters:
        Returns:
        """
        z = '\n{} from instance "{}" on host "{}".'
        z = z.format('{}', self.instance, self.hostname)
        try:
            self.connection.close()
            self.connection = None
            print(z.format('Successfully disconnected'))
        except self.db_library.Error:
            print_stacktrace()
            print(z.format('Failed to disconnect'))
            exit(1)
        return

    def create_cursor(self):
        """ Method that creates a new cursor, and returns it.

        Parameters:
        Returns:
            cursor: handle to this database.
        """
        return self.connection.cursor()

    def commit(self):
        """ Method that issues a commit on this object's connection.

        Parameters:
        Returns:
        """
        return self.connection.commit()

    def print_connection_status(self) -> None:
        """ Method that prints whether or not connected to this database.

        Parameters:
        Returns:
        """
        z = 'Connection status for instance "{}", host "{}": {}connected.'
        z = z.format(self.instance, self.hostname, '{}')
        if self.connection is not None:
            print(z.format(''))
        else:
            print(z.format('not '))
        return

    # DATABASE INFORMATION METHODS.

    def get_db_library_name(self) -> str:
        """ Method to return the name of the needed database library.

        Parameters:
        Returns:
            db_library_name (str): database library name.
        """
        return self.db_library_name

    def get_db_type(self) -> str:
        """ Method to return the database type.

        Parameters:
        Returns:
            db_type (str): database software type.
        """
        return self.db_type

    def print_all_connection_parameters(self) -> None:
        """ Method that executes all print methods of this class.

        Parameters:
        Returns:
        """
        print('The database type is "{}".'.format(self.db_type))
        if self.db_type in file_databases:
            print('The database path is "{}".'.format(self.db_path))
        else:
            print('The database software version is "{}".'.format(
                self.connection.version))
            print('The database username is "{}".'.format(self.username))
            print('The database hostname is "{}".'.format(self.hostname))
            print('The database port number is {}.'.format(self.port_num))
            print('The database instance is "{}".'.format(self.instance))
        self.print_connection_status()
        return


class OutputWriter(object):
    """ Write output to standard out or file.

    Attributes:
        out_file_name (str): relative or absolute path to output file,
            or '' for standard output.
        out_file (object): handle to opened file with name = out_file_name.
        align_col (bool): pad values with spaces to align columns?
        col_sep (str): character(s) to separate columns in output.
            Common choices:
            "" (no characters)
            " " (single space, aka chr(32))
            chr(9) (aka the horizontal tab character)
            "|"
            ","
    """
    def __init__(self, out_file_name: str = '', align_col: bool = True,
                 col_sep: str = ',') -> None:
        """ Constructor method for this class.

        Parameters:
            out_file_name (str): relative or absolute path to output file, or ''
                for standard output.
            align_col (bool): if True, pad columns with spaces in the output so
                they always have the same width.
            col_sep (str): character(s) to separate columns in output.
                Common choices:
                "" (no characters)
                " " (single space, aka chr(32))
                chr(9) (aka the horizontal tab character)
                "|"
                ","
        Returns:
        """
        out_file = None
        if out_file_name == '':
            out_file = sys.stdout
        else:
            try:
                out_file = open(out_file_name, 'w')
            except OSError:
                print_stacktrace()
                exit(1)

        self.out_file_name: str = out_file_name
        self.out_file = out_file
        self.align_col: bool = align_col
        self.col_sep: str = col_sep
        return

    def close_output_file(self):
        """ Close output file, if it exists.

        Parameters:
        Returns:
        """
        if self.out_file_name != '':
            self.out_file.close()
        return

    def get_align_col(self):
        """ Prompt for align_col: chaice to align or not align columns.

        Parameters:
        Returns:
        """
        prompt = '\nAlign columns (pad with blanks)?  Enter "Y" or "N":\n'
        # Keep looping until have acceptable answer.
        while True:
            response = input(prompt).strip().upper()
            if response == '':
                print('Invalid answer, please try again.')
            elif response in 'YT1':
                print('You chose to align columns.')
                self.align_col = True
                break
            elif response in 'NF0':
                print('You chose to not align columns.')
                self.align_col = False
                break
            else:
                print('Invalid answer, please try again.')
        return

    def get_col_sep(self):
        """ Prompt for col_sep: character(s) to separate columns.

        Parameters:
        Returns:
        """
        prompt = '\nEnter column separator character(s):\n'
        self.col_sep = input(prompt)
        print('You chose separate columns with "{}".'.format(self.col_sep))
        return

    def get_out_file_name(self):
        """ Prompt for relative or absolute path to output file.

        Parameters:
        Returns:
        """
        from os.path import dirname, isdir
        prompt = ('\nEnter the name and relative or absolute'
                  '\nlocation of the file to write output to.'
                  '\nOr hit Return to print to the standard output:\n')
        # Keep looping until able to open output file, or "" entered.
        while True:
            out_file_name = input(prompt).strip()
            if out_file_name == '':
                out_file = sys.stdout
                break
            else:
                dir_name = dirname(out_file_name)
                if isdir(dir_name):
                    try:
                        out_file = open(out_file_name, 'w')
                        break
                    except OSError:
                        print_stacktrace()
                else:
                    print('Invalid directory specified: ' + dir_name)

        self.out_file = out_file
        self.out_file_name = out_file_name
        if self.out_file_name == '':
            print('You chose to write to the standard output.')
        else:
            print('Your output file is "{}".'.format(self.out_file_name))
        return

    def write_rows(self, all_rows: list, col_names: list) -> None:
        """ Write rows in the output of SQL to chosen destination.

        Parameters:
            all_rows (list): list of tuples, each tuple a row.
            col_names (list): list of column names.
                None means do not write column headers and line of dashes below.
        Returns:
        """
        # Put quotes around columns containing col_sep.
        if self.col_sep != '':
            # Loop through rows.
            for index1, row in enumerate(all_rows):
                changed = False
                # Loop through columns in each row.
                for index2, column in enumerate(row):
                    # Update row when needed.
                    if self.col_sep in str(column):
                        # Convert tuple to list to make row mutable.
                        if not changed:
                            row = list(row)
                            changed = True
                        # Enclose values containing col_sep in quotes,
                        # double quotes to escape them.
                        row[index2] = "'" + str(column).replace("'", "''") + "'"
                # Save updated version of row if changed = True.
                if changed:
                    all_rows[index1] = tuple(row)

        # Column name widths.
        if col_names is not None:
            col_sizes = [len(col_name) for col_name in col_names]
        else:
            # Not printing column names, use data row 0 to start col_sizes calc.
            col_sizes = [len(row) for row in all_rows[0]]

        if self.align_col:
            # Column widths to align columns, make just wide enough.
            for row in all_rows:
                col_sizes = [max(size, len(str(col))) for (size, col) in
                             zip(col_sizes, row)]

        if col_names is not None:
            # Format and print the column names.
            formats = ['{{:^{}}}'.format(size) for size in col_sizes]
            col_names_fmt = self.col_sep.join(formats)
            self.out_file.write('\n' + col_names_fmt.format(*col_names))

            # Print line of dashes below the column names.
            dashes = ['-' * col_size for col_size in col_sizes]
            self.out_file.write('\n' + self.col_sep.join(dashes))

        # Find format string for the rows.
        if self.align_col:
            formats = ['{{:{}}}'.format(size) for size in col_sizes]
        else:
            formats = ['{}']*len(col_sizes)
        row_fmt = self.col_sep.join(formats)

        # Print the rows.
        self.out_file.writelines(['\n' + row_fmt.format(*r) for r in all_rows])
        # If printed to file, announce that.
        if self.out_file_name != '':
            print('Just wrote output to "{}".'.format(self.out_file_name))
        return


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
        self.db_library_name = self.db_instance.get_db_library_name()
        db_library = __import__(self.db_library_name)
        self.db_library = db_library

        # Get database cursor.
        self.cursor = self.db_instance.create_cursor()

        # Placeholders for SQL text and bind variables.
        # To avoid PyCharm warnings about variables defined outside of __init__.
        self.sql: str = ''
        self.bind_vars = dict()
        return

    def clean_up(self) -> None:
        """ Clean up resources before destroying an instance of this class.

        Parameters:
        Returns:
        """
        print('cleaning up')
        if self.cursor is not None:
            self.cursor.close()
            self.cursor = None
        if self.db_instance is not None:
            self.db_instance.commit()
            self.db_instance = None
        return

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

    def set_sql_at_prompt(self) -> None:
        """ Set text of SQL at a prompt.

        Parameters:
        Returns:
        """
        # Get text of SQL program.
        prompt = ('\nEnter the lines of a SQL program.'
                  '\nHit Return to clean_up entering the SQL.'
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
                self.cursor.execute(self.sql, self.bind_vars)
                self.db_instance.commit()

                # Get row count.
                row_count: int = self.cursor.rowcount

                if self.cursor is None:
                    print('\nCursor is None.')
                    self.clean_up()
                    exit(1)
                elif sql_type in ('INSERT', 'UPDATE', 'DELETE'):
                    pass
                elif not sql_type == 'SELECT':
                    # Not a CRUD statement.
                    pass
                elif sql_type == 'SELECT':
                    # Get column names
                    col_names = [item[0] for item in self.cursor.description]

                    # Fetch rows.  Fetchall for large number of rows a problem.
                    all_rows = self.cursor.fetchall()

                    # In Oracle, cursor.rowcount = 0, so get row count directly.
                    row_count = len(all_rows)
            except self.db_library.Error:
                print_stacktrace()
            finally:
                return col_names, all_rows, row_count

    # THE REST OF THE METHODS ALL HAVE TO DO WITH SEEING THE DATABASE SCHEMA.

    def database_table_schema(self, colsep='|') -> None:
        """ Print the schema for a table.

        Parameters:
            colsep (str): column separator to use.
        Returns:
        """
        # Find tables
        tables = self.find_tables()

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
        output_writerx = OutputWriter(out_file_name='', align_col=True,
                                      col_sep=colsep)

        # Find and print columns in this table.
        columns_col_names, columns_rows = self.find_table_columns(my_table)
        output_writerx.write_rows(columns_rows, columns_col_names)

        # Find all indexes in this table.
        indexes_col_names, indexes_rows = self.find_indexes(my_table)

        # Add 'INDEX_COLUMNS' to end of col_names.
        indexes_col_names.append('INDEX_COLUMNS')

        # Go through indexes, add index_columns to end of each index/row.
        for count, index_row in enumerate(indexes_rows):
            index_name = index_row[0]
            _, ind_col_rows = self.find_index_columns(index_name)
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
        output_writerx.write_rows(indexes_rows, indexes_col_names)
        output_writerx.close_output_file()
        return

    def database_view_schema(self, colsep='|') -> None:
        """ Print the schema for a view.

        Parameters:
            colsep (str): column separator to use.
        Returns:
        """
        # Find views
        views_col_names, views = self.find_views()
        # views_col_names = ['VIEW_NAME', 'VIEW_SQL']

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
        output_writerx = OutputWriter(out_file_name='', align_col=True,
                                      col_sep=colsep)

        # Find and print columns in this view.
        columns_col_names, columns_rows = self.find_view_columns(my_view_name)
        output_writerx.write_rows(columns_rows, columns_col_names)
        output_writerx.close_output_file()
        return

    # EVERYTHING ABOVE IS DATABASE TYPE INDEPENDENT, NOT BELOW.

    def find_tables(self) -> list:
        """ Find the tables in this user's schema.

        Parameters:
        Returns:
            table_names (list): list of the tables in this user's schema.
        """

        # The query for finding the tables in this user's schema.
        sql_x = ''
        bind_vars_x = dict()
        if self.db_type == oracle:
            sql_x = """SELECT table_name
                       FROM user_tables
                       ORDER BY table_name"""

        # Execute the SQL.
        self.set_sql(sql_x, bind_vars_x)
        tables_col_names, tables_rows, tables_row_count = self.run_sql()

        # Return the list of table names.
        return [row[0] for row in tables_rows]

    def find_views(self) -> (list, list):
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
            sql_x = """SELECT view_name, text AS view_sql
                       FROM user_views
                       ORDER BY view_name"""

        # Execute the SQL.
        self.set_sql(sql_x, bind_vars_x)
        views_col_names, views_rows, views_row_count = self.run_sql()

        # Return the list of view names.
        return views_col_names, views_rows

    def find_table_columns(self, table_name: str) -> (list, list):
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

        # Execute the SQL.
        self.set_sql(sql_x, bind_vars_x)
        columns_col_names, columns_rows, columns_row_count = self.run_sql()

        # Replace None by '(null)' everywhere.
        columns_rows = [[no_none(x, '(null)') for x in r] for r in columns_rows]

        # Return the column information.
        return columns_col_names, columns_rows

    def find_view_columns(self, view_name: str) -> (list, list):
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
        return self.find_table_columns(view_name)

    def find_indexes(self, table_name: str) -> (list, list):
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
            sql_x = """SELECT index_name, index_type, table_type, uniqueness
                       FROM user_indexes
                       WHERE table_name = :table_name
                       ORDER BY index_name"""
            bind_vars_x = {"table_name": table_name}

        # Execute the SQL.
        self.set_sql(sql_x, bind_vars_x)
        indexes_col_names, indexes_rows, indexes_row_count = self.run_sql()

        # Return the index information.
        return indexes_col_names, indexes_rows

    def find_index_columns(self, index_name: str) -> (list, list):
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
            sql_x = """
            SELECT ic.column_position, column_name, descend, column_expression
            FROM user_ind_columns ic LEFT OUTER JOIN user_ind_expressions ie
            ON ic.column_position = ie.column_position
            AND ic.index_name = ie.index_name
            WHERE ic.index_name = :index_name
            ORDER BY ic.column_position"""
            bind_vars_x = {"index_name": index_name}

        # Execute the SQL.
        self.set_sql(sql_x, bind_vars_x)
        ind_col_col_names, ind_col_rows, ind_col_row_count = self.run_sql()

        # Return the information about this index.
        return ind_col_col_names, ind_col_rows

# -------- CUSTOM STAND-ALONE FUNCTIONS


def no_none(might_be_none: str, if_its_none: str) -> str:
    """ If argument is None, change it.

    Parameters:
        might_be_none (str): string that might be None.
        if_its_none (str): if might_be_none is None, change it to this.
    Returns:
        its_not_none (str): guaranteed to not be None.
    """
    if might_be_none is None:
        its_not_none = if_its_none
    else:
        its_not_none = might_be_none

    return its_not_none


def print_stacktrace() -> None:
    """ Print a stack trace, then resume.

    Parameters:
    Returns:
    """
    from traceback import print_exception
    print()
    print_exception(*sys.exc_info(), limit=None, file=sys.stdout)
    return


def ask_for_password(username: str) -> str:
    """ Method to ask end-user for password for "username".

    Parameters:
        username (str): the username used to connect to this database instance.
    Returns:
        password (str): the password for "username".
    """
    from getpass import getpass
    prompt = "\nEnter {}'s password: ".format(username)  # Accept anything.
    if sys.stdin.isatty():
        # getpass works in terminal windows, but hangs in PyCharm (to fix, do
        # "Edit configurations" & select "Emulate terminal in output console").
        password = getpass(prompt)
    else:
        # In Eclipse & IDLE, getpass uses "input", which echoes password.
        password = input(prompt)
    return password


def run_sql_cmdline(sql: str, db_type: str) -> list:
    """ Run SQL against database using command line client.

    Parameters:
        sql (str): text of the SQL to run.
        db_type (str): the database type
    Returns:
        sql_output (list): rows of output.
    """
    from subprocess import Popen, PIPE
    if db_type == oracle:
        p = Popen(['sqlplus', '/nolog'],  stdin=PIPE, stdout=PIPE, stderr=PIPE)
        (stdout, stderr) = p.communicate(sql.encode('utf-8'))
        return stdout.decode('utf-8').split("\n")
    elif db_type == sql_server:
        p = Popen(['sqlcmd', '/nolog'],  stdin=PIPE, stdout=PIPE, stderr=PIPE)
        (stdout, stderr) = p.communicate(sql.encode('utf-8'))
        return stdout.decode('utf-8').split("\n")

# -------- IF __NAME__ ....


if __name__ == '__main__':
    # GET DATABASE INSTANCE TO USE.
    db_type1 = oracle
    db_path1 = ''
    username1 = 'ds2'
    password1 = ask_for_password(username1)
    hostname1 = 'DESKTOP-C54UGSE.attlocal.net'
    port_num1 = 1521
    instance1 = 'XE'

    # COLUMN SEPARATOR FOR OUTPUT.
    my_colsep = '|'

    # TEXT OF COMMANDS TO RUN IN SQLPLUS.
    # Explanation of commands:
    # SET SQLPROMPT ""      turn off prompt
    # SET SQLNUMBER OFF     turn off numbers printed for multi-line input
    # SET TRIMOUT ON        trim trailing spaces
    # SET TAB OFF           no tabs in the output
    # SET NEWPAGE 0         no lines between page top and top title
    # SET LINESIZE 256      characters/line
    # SET WRAP OFF          lines don't wrap, truncated to match LINESIZE
    # SET COLSEP "|"        set column separator to pipe character
    # SPOOL <filename>      writes output to <filename> instead of standard out
    # SET TRIMSPOOL ON      trim trailing spaces in output file
    sql_for_sqlplus = """
    SET SQLPROMPT ""
    CONNECT {}/{}@{}:{}/{}
    SET SQLNUMBER OFF
    SET TRIMOUT ON
    SET TAB OFF
    SET NEWPAGE 0
    SET LINESIZE 256
    SET WRAP OFF
    SET COLSEP "{}"
    VARIABLE actor VARCHAR2(50)
    VARIABLE whatever NUMBER
    BEGIN
        :actor := 'CHEVY FOSTER';
    END;
    /
    SELECT actor, title, price, categoryname
    FROM products p INNER JOIN categories c
    ON p.category = c.category
    WHERE actor = :actor;
    exit
    """.format(username1, password1, hostname1, port_num1, instance1, my_colsep)

    # RUN ABOVE COMMANDS IN SQLPLUS.
    print('\nRUNNING COMMANDS IN SQLPLUS...')
    sqlplus_output = run_sql_cmdline(sql_for_sqlplus, oracle)

    # SHOW THE OUTPUT FROM RUNNING ABOVE COMMANDS IN SQLPLUS.
    # Don't use write_rows, it'll crash because sqlplus output not all columnar.
    print('\nTHE OUTPUT FROM RUNNING THOSE COMMANDS IN SQLPLUS:')
    for line in sqlplus_output:
        print(line)
    print('ALL DONE WITH THAT OUTPUT.')

    # CONNECT TO DATABASE INSTANCE SPECIFIED ABOVE.
    print('\nCONNECTING TO DATABASE...')
    db_instance1 = DBInstance(db_type1, db_path1, username1, password1,
                              hostname1, port_num1, instance1)
    db_instance1.print_all_connection_parameters()

    # CREATE DATABASE CLIENT OBJECT.
    print('\nGETTING DATABASE CLIENT OBJECT...')
    my_db_client = DBClient(db_instance1)

    # See the database table schema for my login.
    print('\nSEE THE COLUMNS AND INDEXES OF ONE TABLE...')
    my_db_client.database_table_schema(colsep=my_colsep)
    print()

    # See the database view schema for my login.
    print('\nSEE THE DEFINITION AND COLUMNS OF ONE VIEW...')
    my_db_client.database_view_schema(colsep=my_colsep)
    print()

    # Pass in text of SQL and a dict of bind variables and their values.
    sql1 = """SELECT actor, title, price, categoryname
               FROM products p INNER JOIN categories c
               ON p.category = c.category
               WHERE actor = :actor"""
    print("\nHERE'S SOME SQL:\n{}".format(sql1))
    bind_vars1 = {'actor': 'CHEVY FOSTER'}
    print('\nHERE ARE BIND VARIABLES:\n{}'.format(bind_vars1))
    my_db_client.set_sql(sql1, bind_vars1)

    # Execute the SQL.
    print('\nGETTING THE OUTPUT OF THAT SQL:')
    col_names1, rows1, row_count1 = my_db_client.run_sql()

    # Set up to write output.
    print('\nPREPARING TO FORMAT THAT OUTPUT, AND PRINT OR WRITE IT TO FILE.')
    output_writer = OutputWriter(out_file_name='', align_col=True, col_sep='|')
    output_writer.get_align_col()
    output_writer.get_col_sep()
    output_writer.get_out_file_name()
    # Show the results.
    print("\nHERE'S THE OUTPUT...")
    output_writer.write_rows(rows1, col_names1)

    # Clean up.
    print('\n\nOK, FORGET ALL THAT.')
    output_writer.close_output_file()
    col_names1 = None
    rows1 = None

    # From a prompt, read in SQL & dict of bind variables & their values.
    my_db_client.set_sql_at_prompt()
    my_db_client.set_bind_vars_at_prompt()

    # Execute the SQL.
    print('\nGETTING THE OUTPUT OF THAT SQL:')
    col_names2, rows2, row_count2 = my_db_client.run_sql()

    # Set up to write output.
    print('\nPREPARING TO FORMAT THAT OUTPUT, AND PRINT OR WRITE IT TO FILE.')
    output_writer = OutputWriter(out_file_name='', align_col=True, col_sep='|')
    output_writer.get_align_col()
    output_writer.get_col_sep()
    output_writer.get_out_file_name()
    # Show the results.
    output_writer.write_rows(rows2, col_names2)

    # Clean up.
    output_writer.close_output_file()
    col_names2 = None
    rows2 = None
    print()
    db_instance1.close_connection()
    db_instance1.print_connection_status()
