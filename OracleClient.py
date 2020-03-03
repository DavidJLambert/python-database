""" OracleClient.py

SUMMARY:
  Class OracleClient executes PL/SQL with bind variables.

REPOSITORY: https://github.com/DavidJLambert/Python-Universal-DB-Client

AUTHOR: David J. Lambert

VERSION: 0.3.0

DATE: Mar 3, 2020

PURPOSE:
  A sample of my Python coding, to demonstrate that I can write decent Python,
  test it, and document it.  I also demonstrate I know relational databases.

DESCRIPTION:
  Class OracleDB contains all the information needed to log into an Oracle
  instance, plus it contains the connection handle to that database.

  Class OracleClient executes PL/SQL with bind variables, and then prints
  out the results.  Its externally useful methods are:
  1)  set_plsql_text: gets the text of PL/SQL to run, including bind variables.
  2)  get_plsql_text_from_command_line: reads text of PL/SQL to run from the
      command line.
  3)  get_bind_vars_from_command_line: reads the bind variables and their values
      from the command line.
  4)  run_plsql: executes PL/SQL, regardless of whether it was read as a text
      variable (with set_plsql_text) or entered at the command line (by
      get_plsql_text_from_command_line and get_bind_vars_from_command_line).
  5)  oracle_table_schema: lists all the tables owned by the current login, all
      the columns in those tables, and all indexes on those tables.

  Stand-alone Method run_sqlplus runs sqlplus as a subprocess.

  The code has been tested with CRUD statements (Create, Read, Update, Delete).
  There is nothing to prevent the end-user from entering other PL/SQL, such as
  ALTER DATABASE, CREATE VIEW, and BEGIN TRANSACTION, but none have been tested.

  This program loads the result set into memory.  Thus, it is not suitable for
  results sets that drive the host machine's available RAM to zero.

PROGRAM REQUIREMENTS:

  + For connecting to Oracle, my code uses the cx_Oracle library, which is
    available on PyPI.  The cx_Oracle library requires the Oracle client
    libraries.  Several ways to obtain the Oracle client libraries are
    documented on https://cx-oracle.readthedocs.io/en/latest/installation.html.

    Cx_Oracle v7.3.0 supports Python versions 2.7 and 3.5-3.8,
    and Oracle client versions 11.2-19.

SAMPLE DATABASES TO TEST THIS PROGRAM ON:
  The sample database has the same data: the small version of the Dell
  DVD Store database, version 2.1, available at http://linux.dell.com/dvdstore.
  The data is in these tables:

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
from traceback import print_exception
from getpass import getpass
from datetime import datetime
import sys
import cx_Oracle
import subprocess
import struct
import platform

# -------- CREATE AND INITIALIZE VARIABLES

ARRAY_SIZE = 20

# -------- OS INFORMATION STUFF

uname = platform.uname()
z = ('OS: {}\nHost Name: {}\nOS Major Version: {}\nOS Full Version: {}'
     '\nProcessor Type: {}\nProcessor: {}')
result = z.format(uname.system, uname.node, uname.release, uname.version,
                  uname.machine, uname.processor)
print(result)

# -------- PYTHON VERSION STUFF

sys_version_info = sys.version_info
bits = 8*struct.calcsize("P")
version = '.'.join(str(x) for x in sys_version_info)
z = '\n{} Version {}, {} bits.'
print(z.format(platform.python_implementation(), version, bits))

# -------- CUSTOM CLASSES


class OracleDB(object):
    """ Class containing Oracle database connection utilities and information
        about one Oracle database instance (referred to as "this database").

    Attributes:
        username (str): a username for connecting to this database.
        password (str): the password for "username".
        hostname (str): the hostname of this database.
        port_num (int): the port this database listens on.
        instance (str): the name of this database instance.
        connection_string (str): the connection string for this database.
        connection: the handle to this database. I set connection = None when
                    connection closed, this is not default behavior.
    """

    def __init__(self, username: str, password: str, hostname: str,
                 port_num: int, instance: str) -> None:
        """ Constructor method for this class.

        Parameters:
            username (str): the username for the connection to this database.
            password (str): the password for "username".
            hostname (str): the hostname of this database.
            port_num (int): the port this database listens on.
            instance (str): the name of this database instance.
        Returns:
        """
        # Database information.
        self.username: str = username
        self.password: str = password
        self.hostname: str = hostname
        self.port_num: int = port_num
        self.instance: str = instance
        # Get ready to connect to database.
        z = '{}/{}@{}:{}/{}'
        self.connection_string = z.format(username, password, hostname,
                                          port_num, instance)
        self.connection = None

    # DATABASE CONNECTION METHODS.

    def open_connection(self) -> None:
        """ Method for opening a connection to this database.

        Parameters:
        Returns:
        """
        try:
            self.connection = cx_Oracle.connect(self.connection_string)
            z = 'Successfully connected to instance "{}" on host "{}".'
            print(z.format(self.get_instance(), self.get_hostname()))
        except Exception:
            print_stacktrace()
            z = 'Failed to connect to instance "{}" on host "{}".'
            print(z.format(self.get_instance(), self.get_hostname()))
            exit(1)

    def close_connection(self) -> None:
        """ Method to close connection to this database.

        Parameters:
        Returns:
        """
        try:
            self.connection.close()
            self.connection = None
            z = 'Successfully disconnected from instance "{}" on host "{}".'
            print(z.format(self.get_instance(), self.get_hostname()))
        except Exception:
            print_stacktrace()
            z = 'Failed to disconnect from instance "{}" on host "{}".'
            print(z.format(self.get_instance(), self.get_hostname()))
            exit(1)

    def get_connection_object(self):
        """ Method that returns handle (connection) to this database.

        Parameters:
        Returns:
            connection: handle to this database.
        """
        return self.connection

    def get_connection_status(self) -> bool:
        """ Method that returns boolean saying if connected to this database.

        Parameters:
        Returns:
            Boolean: whether or not connected to this database.
        """
        return self.connection is not None

    def print_connection_status(self) -> None:
        """ Method that prints whether or not connected to this database.

        Parameters:
        Returns:
        """
        if self.get_connection_status():
            z = 'Connection status for instance "{}", host "{}": connected.'
        else:
            z = 'Connection status for instance "{}", host "{}": not connected.'
        print(z.format(self.get_instance(), self.get_hostname()))

    # DATABASE INFORMATION METHODS.

    def get_database_version(self) -> str:
        """ Method to return Oracle version of this database.

        Parameters:
        Returns:
            version (str): Oracle database version.
        """
        return self.connection.version

    def print_database_version(self) -> None:
        """ Method to print Oracle version of this database.

        Parameters:
        Returns:
        """
        print('The Oracle version is "{}".'.format(self.get_database_version()))

    def get_username(self) -> str:
        """ Method to return username connecting to this database.

        Parameters:
        Returns:
            username (str): username connecting to this database.
        """
        return self.username

    def print_username(self) -> None:
        """ Method to print username connecting to this database.

        Parameters:
        Returns:
        """
        print('The Oracle username is "{}".'.format(self.get_username()))

    def get_hostname(self) -> str:
        """ Method to return hostname of this database.

        Parameters:
        Returns:
            hostname (str): hostname of this database.
        """
        return self.hostname

    def print_hostname(self) -> None:
        """ Method to print hostname of this database.

        Parameters:
        Returns:
        """
        print('The Oracle hostname is "{}".'.format(self.get_hostname()))

    def get_port_num(self) -> int:
        """ Method to return the port number this database listens on.

        Parameters:
        Returns:
            port_num (int): the port number this database listens on.
        """
        return self.port_num

    def print_port_num(self) -> None:
        """ Method to print port_num this database listens on.

        Parameters:
        Returns:
        """
        print('The Oracle port number is {}.'.format(self.get_port_num()))

    def get_instance(self) -> str:
        """ Method to return instance name of this database.

        Parameters:
        Returns:
            instance (str): instance name of this database.
        """
        return self.instance

    def print_instance(self) -> None:
        """ Method to print instance name of this database.

        Parameters:
        Returns:
        """
        print('The Oracle instance is "{}".'.format(self.get_instance()))

    def print_all_connection_parameters(self) -> None:
        """ Method that executes all print methods of this class.

        Parameters:
        Returns:
        """
        self.print_database_version()
        self.print_username()
        self.print_hostname()
        self.print_port_num()
        self.print_instance()
        self.print_connection_status()


class OracleClient(object):
    """ Get text of a PL/SQL program with bind variables, then execute it.

    Attributes:
        plsql (str): the text of a PL/SQL program with bind variables.
        bind_var_dict (dict): the bind variables' names and values.
        cursor: the cursor for this PL/SQL when it executes. I set cursor = None
                when cursor closed.
        connection: the handle to this database.
    """

    def __init__(self, database) -> None:
        """ Constructor method for this class.

        Parameters:
            database: the handle for this database.
        Returns:
        """
        # Get Connection from my_database object.
        self.connection = database.get_connection_object()

        # Placeholders for PL/SQL text, cursor, dict of bind variable values.
        # To avoid PyCharm warnings about variables defined outside of __init__.
        self.plsql: str = ''
        self.cursor = None
        self.bind_var_dict = dict()

    def cleanup(self) -> None:
        """ Clean up resources before destroying an instance of this class.

        Parameters:
        Returns:
        """
        if self.cursor is not None:
            self.cursor.close()
            self.cursor = None
        if self.connection is not None:
            self.connection.commit()

    def set_plsql_text(self, plsql: str, bind_var_dict: dict) -> None:
        """ Set text of PL/SQL to execute + values of any bind variables in it.

        Parameters:
            plsql (str): text of the PL/SQL to execute.
            bind_var_dict (dict): the bind variables' names and values.
        Returns:
        """
        self.plsql: str = plsql
        self.bind_var_dict: dict = bind_var_dict

    def get_plsql_text_from_command_line(self) -> None:
        """ Get text of PL/SQL at the command line.

        Parameters:
        Returns:
        """
        # Get text of PL/SQL program.
        prompt = ('\nEnter the current PL/SQL program.'
                  '\nHit Return to finish entering the PL/SQL.'
                  '\nAt any point, enter "Q" to Quit this program.\n')
        plsql = ''
        while True:
            # Get the next line of PL/SQL.
            response = input(prompt).strip()
            # Use a prompt only for the first line of PL/SQL.
            prompt = ''

            if response == '':
                # Done entering PL/SQL.
                break
            elif response.upper() == 'Q':
                print('\nQuitting at your request.')
                self.cleanup()
                exit(0)
            else:
                # Add more text to the current PL/SQL.
                plsql += '\n' + response
        self.plsql = plsql.strip()

    def get_bind_vars_from_command_line(self) -> None:
        """ Get bind variables at the command line.

        Parameters:
        Returns:
        """
        # Prompts for calling input().
        prompt_name = ("\nEnter a bind variable name."
                       "\nOr hit Return when done entering bind variables.\n")
        prompt_type = ("\nEnter the letter of the bind variable's data type:"
                       "\n(T)ext."
                       "\n(I)nteger."
                       "\n(R)eal (aka decimal)."
                       "\n(D)ate.\n")
        prompt_value = "\nEnter the bind variable value.\n"
        prompt_text = "Do not put quotes around text, that's done for you.\n"
        prompt_date = "Use the date format 'm/d/yyyy'.\n"

        # Dict with keys = bind variable names, values = bind variable values.
        bind_var_dict = dict()
        # One loop per bind variable name/value pair.
        while True:
            # Get bind variable name.
            bind_var_name = input(prompt_name).strip()
            if bind_var_name == '':
                # Done entering PL/SQL.
                break
            elif bind_var_name.upper() == 'Q':
                print('\nQuitting at your request.')
                self.cleanup()
                exit(0)

            # Get bind variable data type.
            while True:
                bind_var_type = input(prompt_type).strip().upper()
                if len(bind_var_type) != 1:
                    print('Enter one character only, please try again.')
                elif bind_var_type not in 'TIRD':
                    print('Invalid entry, please try again.')
                else:
                    # Valid input.
                    break

            # Get bind variable value.
            if bind_var_type == 'T':
                bind_var_value = input(prompt_value + prompt_text).strip()
            elif bind_var_type == 'I':
                bind_var_value = int(input(prompt_value).strip())
            elif bind_var_type == 'R':
                bind_var_value = float(input(prompt_value).strip())
            elif bind_var_type == 'D':
                while True:
                    bind_var_value = input(prompt_value + prompt_date).strip()
                    try:
                        bind_var_value = datetime.strptime(bind_var_value,
                                                           '%m/%d/%Y')
                        break
                    except ValueError:
                        print('Invalid date, please try again.')
            else:
                print('\nProgram bug.')
                self.cleanup()
                exit(0)

            # Add bind variable name/value pair to dictionary of bind variables.
            # Ignore PyCharm warning bind_var_value might be referenced before
            # assignment.
            bind_var_dict[bind_var_name] = bind_var_value

        # All done!
        self.bind_var_dict = bind_var_dict

    def run_plsql(self) -> (list, list, int):
        """ Run the PL/SQL, perhaps return rows and column names.

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
        if not self.plsql:
            # No plsql to execute.
            pass
        else:
            try:
                # Classify PL/SQL.
                plsql_type: str = self.plsql.split()[0].upper()

                # Get cursor.
                self.cursor = self.connection.cursor()

                # Execute PL/SQL.
                self.cursor.execute(self.plsql, self.bind_var_dict)
                self.connection.commit()

                # Get row count.
                row_count: int = self.cursor.rowcount

                if self.cursor is None:
                    print('\nCursor is None.')
                    self.cleanup()
                    exit(1)
                elif plsql_type in ('INSERT', 'UPDATE', 'DELETE'):
                    pass
                elif not plsql_type == 'SELECT':
                    # Not a CRUD statement.
                    pass
                elif plsql_type == 'SELECT':
                    # Get column names
                    col_names = [item[0] for item in self.cursor.description]

                    # Fetch rows.  Fetchall for large number of rows a problem.
                    all_rows = self.cursor.fetchall()

                    # In Oracle, cursor.rowcount = 0, so get row count directly.
                    row_count = len(all_rows)
            except Exception:
                print_stacktrace()
            finally:
                self.cleanup()
                return col_names, all_rows, row_count

    # THE REST OF THE METHODS ALL HAVE TO DO WITH SEEING THE ORACLE SCHEMA.

    def find_tables(self) -> list:
        """ Find the tables in this user's schema.

        Parameters:
        Returns:
            table_names (list): list of the tables in this user's schema.
        """

        # The query for finding the tables in this user's schema.
        plsql_x = """SELECT table_name
                     FROM user_tables
                     ORDER BY table_name"""
        bind_var_dict_x = dict()
        # CLUSTER_NAME
        # IOT_NAME
        # IOT_TYPE

        # Execute the PL/SQL.
        self.set_plsql_text(plsql_x, bind_var_dict_x)
        tables_col_names, tables_rows, tables_row_count = self.run_plsql()

        # Return the list of table names.
        return [row[0] for row in tables_rows]

    def find_views(self) -> list:
        """ Find the views in this user's schema.

        Parameters:
        Returns:
            view_names (list): list of the views in this user's schema.
        """

        # The query for finding the tables in this user's schema.
        plsql_x = """SELECT view_name
                     FROM user_views
                     ORDER BY view_name"""
        bind_var_dict_x = dict()
        # TEXT_LENGTH
        # TEXT
        # TYPE_TEXT_LENGTH
        # TYPE_TEXT
        # VIEW_TYPE

        # Execute the PL/SQL.
        self.set_plsql_text(plsql_x, bind_var_dict_x)
        views_col_names, views_rows, views_row_count = self.run_plsql()

        # Return the list of table names.
        return [row[0] for row in views_rows]

    def find_table_columns(self, table_name: str) -> (list, list):
        """ Find the columns in a table.

        Parameters:
            table_name (str): the table to find the columns of.
        Returns:
            col_names (list): column names: [column_id, column_name, data_type]
            rows (list): list of tuples, each tuple holds info about one column:
                tuple = (column_id, column_name, data_type).
        """

        # The SQL to find the columns, their order, and their data types.
        plsql_x = """SELECT column_id, column_name, data_type, data_length,
                            data_precision, data_scale, char_used
                     FROM user_tab_cols
                     WHERE table_name = :table_name
                     ORDER BY column_id"""

        # Execute the PL/SQL.
        bind_var_dict_x = {"table_name": table_name}
        self.set_plsql_text(plsql_x, bind_var_dict_x)
        columns_col_names, columns_rows, columns_row_count = self.run_plsql()

        # We got data_type, data_length, data_precision, data_scale, and
        # char_used from user_tab_cols.  Now to translate that into the data
        # type we usually see when we run DESCRIBE <TABLE_NAME>.

        # Create mapping from column name to index number, so I can access
        # items in col_names by column name instead of by index number.
        columns = {col_name: count for count, col_name in
                   enumerate(columns_col_names)}

        # Put the human-readable data type in data_type.
        for count, row in enumerate(columns_rows):
            data_type = row[columns['DATA_TYPE']]
            if data_type in ('BLOB', 'NCLOB', 'CLOB', 'DATE', 'BINARY_FLOAT',
                             'BINARY_DOUBLE', 'LONG', 'LONG RAW'):
                pass
            elif data_type == 'BFILE':
                data_type = 'BINARY FILE LOB (BFILE)'
            elif data_type[0:8] in ('TIMESTAMP', 'INTERVAL '):
                pass
            elif data_type in ('NCHAR', 'NVARCHAR2'):
                data_length = row[columns['DATA_LENGTH']]
                data_type += '(' + str(data_length//2) + ')'
            elif data_type in ('VARCHAR2', 'RAW', 'UROWID', 'CHAR'):
                data_length = row[columns['DATA_LENGTH']]
                if row[columns['CHAR_USED']] == 'C':
                    data_length = str(data_length//4) + ' CHAR'
                data_type += '(' + str(data_length) + ')'
            elif data_type == 'NUMBER':
                data_precision = row[columns['DATA_PRECISION']]
                data_scale = row[columns['DATA_SCALE']]
                if data_precision is not None:
                    data_type += '(' + str(data_precision)
                    if data_scale > 0:
                        data_type += ',' + str(data_scale) + ')'
                    else:
                        data_type += ')'
                elif data_scale == 0:
                    data_type += '(38) (integer)'
            elif data_type == 'FLOAT':
                data_precision = row[columns['DATA_PRECISION']]
                data_type += '(' + str(data_precision) + ')'
                if data_precision == 126:
                    data_type += ' (double precision)'
                elif data_precision == 63:
                    data_type += ' (real)'
            # Overwrite current row.
            # COLUMN_ID and COLUMN_NAME unchanged, DATA_TYPE different.
            columns_rows[count] = (row[columns['COLUMN_ID']],
                                   row[columns['COLUMN_NAME']], data_type)

        # Return the column information.
        return columns_col_names[0:2], columns_rows

    def find_view_columns(self, view_name: str) -> (list, list):
        """ Find the columns in a view.

        Parameters:
            view_name (str): the table to find the columns of.
        Returns:
            col_names (list): column names: [column_id, column_name, data_type]
            rows (list): list of tuples, each tuple holds info about one column:
                tuple = (column_id, column_name, data_type).
        """
    """
    select
    col.column_id,
    col.owner as schema_name,
    col.table_name,
    col.column_name,
    col.data_type,
    col.data_length,
    col.data_precision,
    col.data_scale,
    col.nullable
    from sys.all_tab_columns col
    inner join sys.all_views v on
        col.owner = v.owner
        and col.table_name = v.view_name
    order by col.owner, col.table_name, col.column_id;
    """

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
        plsql_x = """SELECT index_name, index_type, table_type, uniqueness
                     FROM user_indexes
                     WHERE table_name = :table_name
                     ORDER BY index_name"""
        bind_var_dict_x = {"table_name": table_name}

        # Execute the PL/SQL.
        self.set_plsql_text(plsql_x, bind_var_dict_x)
        indexes_col_names, indexes_rows, indexes_row_count = self.run_plsql()

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
        plsql_x = """SELECT ic.column_position, column_name, descend,
                            column_expression
                     FROM user_ind_columns ic
                          LEFT OUTER JOIN user_ind_expressions ie
                     ON ic.column_position = ie.column_position
                     AND ic.index_name = ie.index_name
                     WHERE ic.index_name = :index_name
                     ORDER BY ic.column_position"""
        bind_var_dict_x = {"index_name": index_name}

        # Execute the PL/SQL.
        self.set_plsql_text(plsql_x, bind_var_dict_x)
        ind_col_col_names, ind_col_rows, ind_col_row_count = self.run_plsql()

        # Return the information about this index.
        return ind_col_col_names, ind_col_rows

    def oracle_table_schema(self) -> None:
        """ Print the schema for a table.

        Parameters:
        Returns:
        """
        # Find tables
        tables = self.find_tables()

        # Ask end-user to choose a table.
        prompt = '\nHere are the tables available to you:\n'
        for count, table in enumerate(tables):
            prompt += str(count) + ': ' + table + '\n'
        prompt += ('Enter the number for the table you want info about:\n'
                   'Or enter "Q" to quit.\n')
        choice = input(prompt).strip().upper()

        # Interpret the choice.
        if choice == "Q":
            exit(0)
        choice = int(choice)
        if choice not in range(len(tables)):
            exit(0)
        my_table = tables[choice]
        tables = None

        # Find and print columns in this table.
        columns_col_names, columns_rows = self.find_table_columns(my_table)
        print_rows(columns_col_names, columns_rows, align_col=True, col_sep=' ')
        columns_col_names = None
        columns_rows = None

        # Find all indexes in this table.
        indexes_col_names, indexes_rows = self.find_indexes(my_table)

        # Add 'INDEX_COLUMNS' to end of col_names.
        indexes_col_names.append('INDEX_COLUMNS')

        # Go through indexes, add index_columns to end of each index/row.
        for count, index in enumerate(indexes_rows):
            index_name = index[0]
            ignore, ind_col_rows = self.find_index_columns(index_name)
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
            ignore = None
            ind_col_rows = None
            # Add index_columns to end of each index/row (index is a tuple!).
            indexes_rows[count] = index + (index_columns,)

        print_rows(indexes_col_names, indexes_rows, align_col=True, col_sep=' ')
        indexes_col_names = None
        indexes_rows = None

    def oracle_view_schema(self) -> None:
        """ Print the schema for a view.

        Parameters:
        Returns:
        """
        # Find views
        views = self.find_views()


# -------- CUSTOM STAND-ALONE FUNCTIONS


def print_stacktrace() -> None:
    """ Print a stack trace, then resume.

    Parameters:
    Returns:
    """
    print()
    print_exception(*sys.exc_info(), limit=None, file=sys.stdout)


def exception_raised() -> bool:
    """ Has an Exception been raised but not handled?

    Parameters:
    Returns:
        (boolean) whether or not an Exception has been raised.
    """
    return sys.exc_info() != (None, None, None)


def ask_for_password(username: str) -> str:
    """ Method to ask end-user for password for "username".

    Parameters:
        username (str): the username for the connection to this database.
    Returns:
        password (str): the password for "username".
    """
    prompt = "Enter {}'s password: ".format(username)  # Accept anything.
    if sys.stdin.isatty():
        # getpass works in terminal windows, but hangs in PyCharm (to fix, do
        # "Edit configurations" & select "Emulate terminal in output console").
        password = getpass(prompt)
    else:
        # In Eclipse & IDLE, getpass uses "input", which echoes password.
        password = input(prompt)
    return password


def print_rows(col_names: list, all_rows: list, align_col: bool = True,
               col_sep: str = ' ') -> None:
    """ Print rows in the output of PL/SQL.

    Parameters:
        col_names (list): list of column names.
        all_rows (list): list of tuples, each tuple a row.
        align_col (bool): if True, align columns in the output so they
            always have the same width, padded by spaces.
        col_sep (str): column separator in output.  Common values are:
                " " (single space, aka chr(32))
                chr(9) (aka the horizontal tab character)
                "|"
                ","
    Returns:
    """

    # Adjust columns containing col_sep.
    if col_sep != '':
        # Loop through rows.
        for count1, row in enumerate(all_rows):
            changed = False
            # Loop through columns in each row.
            for count2, column in enumerate(row):
                # Update row when needed.
                if col_sep in str(column):
                    # Convert tuple to list to make row mutable.
                    if not changed:
                        row = list(row)
                        changed = True
                    # Must enclose values containing col_sep in quotes,
                    # and must double quotes to escape them.
                    row[count2] = "'" + str(column).replace("'", "''") + "'"
            # Save updated version of row if changed = True.
            if changed:
                all_rows[count1] = tuple(row)

    # Column name widths.
    col_sizes = [len(col_name) for col_name in col_names]

    if align_col:
        # Column widths to align columns, make just wide enough.
        for row in all_rows:
            col_sizes = [max(size, len(str(col))) for (size, col) in
                         zip(col_sizes, row)]

    # Format and print the column names.
    formats = ['{{:^{}}}'.format(size) for size in col_sizes]
    col_names_fmt = col_sep.join(formats)
    print('\n' + col_names_fmt.format(*col_names))

    # Print line of dashes below the column names.
    dashes = ['-' * col_size for col_size in col_sizes]
    print(col_sep.join(dashes))

    # Find format string for the rows.
    if align_col:
        formats = ['{{:{}}}'.format(size) for size in col_sizes]
    else:
        formats = ['{}']*len(col_sizes)
    row_output_fmt = col_sep.join(formats)

    # Print the rows.
    [print(row_output_fmt.format(*row)) for row in all_rows]


def run_sqlplus(plsql: str) -> list:
    """ Run PL/SQL against database using sqlplus.

    Parameters:
        plsql (str): text of the PL/SQL to run.
    Returns:
        plsql_output (list): rows of output.
    """

    p = subprocess.Popen(['sqlplus','/nolog'],
                         stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    (stdout, stderr) = p.communicate(plsql.encode('utf-8'))

    return stdout.decode('utf-8').split("\n")

# -------- IF __NAME__ ....


if __name__ == '__main__':
    # Get Oracle instance to use.
    username1 = 'ds2'
    password1 = ask_for_password(username1)
    hostname1 = 'DESKTOP-C54UGSE.attlocal.net'
    port_num1 = 1521
    instance1 = 'XE'

    # Text of commands to run in sqlplus.
    plsql_for_sqlplus = """
    CONNECT {}/{}@{}:{}/{}
    -- trim trailing spaces
    SET TRIMOUT ON
    -- no tabs in the output
    SET TAB OFF
    -- no lines between page top and top title
    SET NEWPAGE 0
    -- characters/line
    SET LINESIZE 256
    -- lines don't wrap, truncated to match LINESIZE
    SET WRAP OFF
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
    """.format(username1, password1, hostname1, port_num1, instance1)

    # Run above commands in sqlplus.
    sqlplus_output = run_sqlplus(plsql_for_sqlplus)

    # Show the output from running above commands in sqlplus.
    for line in sqlplus_output:
        print(line)

    # Set up to connect to Oracle instance specified above.
    database1 = OracleDB(username1, password1, hostname1, port_num1, instance1)

    # Connect to that Oracle instance.
    database1.open_connection()
    database1.print_all_connection_parameters()

    # Pass in database connection to my Oracle Client.
    my_oracle_client = OracleClient(database1)

    # See the Oracle schema for my login.
    my_oracle_client.oracle_table_schema()

    # Pass in text of PL/SQL and a dict of bind variables and their values.
    plsql1 = """SELECT actor, title, price, categoryname
               FROM products p INNER JOIN categories c
               ON p.category = c.category
               WHERE actor = :actor"""
    bind_var_dict1 = {'actor': 'CHEVY FOSTER'}
    my_oracle_client.set_plsql_text(plsql1, bind_var_dict1)

    # Execute the PL/SQL.
    col_names1, rows1, row_count1 = my_oracle_client.run_plsql()

    # Show the results.
    print_rows(col_names1, rows1, align_col=True, col_sep=' ')

    # Clean up.
    col_names1 = None
    rows1 = None
    print()

    # From command line, read in PL/SQL & dict of bind variables & their values.
    my_oracle_client.get_plsql_text_from_command_line()
    my_oracle_client.get_bind_vars_from_command_line()

    # Execute the PL/SQL.
    col_names1, rows1, row_count1 = my_oracle_client.run_plsql()

    # Show the results.
    print_rows(col_names1, rows1, align_col=True, col_sep=' ')

    # Clean up.
    col_names1 = None
    rows1 = None
    print()
    database1.close_connection()
    database1.print_connection_status()
