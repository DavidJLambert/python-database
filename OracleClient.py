""" OracleClient.py

SUMMARY:
  Class OracleClient executes PL/SQL with bind variables.

REPOSITORY: https://github.com/DavidJLambert/Python-Universal-DB-Client

AUTHOR: David J. Lambert

VERSION: 0.4.0

DATE: Mar 6, 2020

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
  3)  get_bindvars_from_command_line: reads the bind variables and their values
      from the command line.
  4)  run_plsql: executes PL/SQL, regardless of whether it was read as a text
      variable (with set_plsql_text) or entered at the command line (by
      get_plsql_text_from_command_line and get_bindvars_from_command_line).
  5)  oracle_table_schema: lists all the tables owned by the current login, all
      the columns in those tables, and all indexes on those tables.
  6)  oracle_view_schema: lists all the views owned by the current login, all
      the columns in those views, and the SQL for the view.

  Class OutputWriter handles all query output to file or to standard output.

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
from os import path
import cx_Oracle
import subprocess
import struct
import platform

# -------- CREATE AND INITIALIZE VARIABLES

ARRAY_SIZE = 20

# -------- OS INFORMATION STUFF

uname = platform.uname()
q = ('OS: {}\nHost Name: {}\nOS Major Version: {}\nOS Full Version: {}'
     '\nProcessor Type: {}\nProcessor: {}')
result = q.format(uname.system, uname.node, uname.release, uname.version,
                  uname.machine, uname.processor)
print(result)

# -------- PYTHON VERSION STUFF

sys_version_info = sys.version_info
bits = 8*struct.calcsize("P")
version = '.'.join(str(x) for x in sys_version_info)
q = '\n{} Version {}, {} bits.'
print(q.format(platform.python_implementation(), version, bits))

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
        return

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
        return

    def close_connection(self) -> None:
        """ Method to close connection to this database.

        Parameters:
        Returns:
        """
        try:
            self.connection.close()
            self.connection = None
            z = '\nSuccessfully disconnected from instance "{}" on host "{}".'
            print(z.format(self.get_instance(), self.get_hostname()))
        except Exception:
            print_stacktrace()
            z = 'Failed to disconnect from instance "{}" on host "{}".'
            print(z.format(self.get_instance(), self.get_hostname()))
            exit(1)
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
        return

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
            except Exception:
                print_stacktrace()
                exit(1)

        self.out_file_name: str = out_file_name
        self.out_file = out_file
        self.align_col: bool = align_col
        self.col_sep: str = col_sep
        return

    def get_align_col(self):
        """ Prompt for align_col: chaice to align or not align columns.

        Parameters:
        Returns:
        """
        prompt = '\nAlign columns (pad with blanks)?  Enter "Y" or "N":\n'
        self.align_col = (input(prompt).strip().upper() in 'YT1')
        if self.align_col:
            print('You chose to align columns.')
        else:
            print('You chose to not align columns.')
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
                dir_name = path.dirname(out_file_name)
                if path.isdir(dir_name):
                    try:
                        out_file = open(out_file_name, 'w')
                        break
                    except Exception:
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
        """ Write rows in the output of PL/SQL to chosen destination.

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
                        # Must enclose values containing col_sep in quotes,
                        # and must double quotes to escape them.
                        row[index2] = "'" + str(column).replace("'", "''") + "'"
                # Save updated version of row if changed = True.
                if changed:
                    all_rows[index1] = tuple(row)

        # Column name widths.
        if col_names is not None:
            col_sizes = [len(col_name) for col_name in col_names]
        else:
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

    def finish_up(self):
        """ Close output file, if it exists.

        Parameters:
        Returns:
        """
        if self.out_file_name != '':
            self.out_file.close()
        return


class OracleClient(object):
    """ Get text of a PL/SQL program with bind variables, then execute it.

    Attributes:
        plsql (str): the text of a PL/SQL program with bind variables.
        bindvar_dict (dict): the bind variables' names and values.
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
        self.bindvar_dict = dict()
        return

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
        return

    def set_plsql_text(self, plsql: str, bindvar_dict: dict) -> None:
        """ Set text of PL/SQL to execute + values of any bind variables in it.

        Parameters:
            plsql (str): text of the PL/SQL to execute.
            bindvar_dict (dict): the bind variables' names and values.
        Returns:
        """
        self.plsql: str = plsql
        self.bindvar_dict: dict = bindvar_dict
        return

    def get_plsql_text_from_command_line(self) -> None:
        """ Get text of PL/SQL at the command line.

        Parameters:
        Returns:
        """
        # Get text of PL/SQL program.
        prompt = ('\nEnter the lines of a PL/SQL program.'
                  '\nHit Return to finish entering the PL/SQL.'
                  '\nAt any point, enter "Q" to Quit this program:\n')
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
        return

    def get_bindvars_from_command_line(self) -> None:
        """ Get bind variables at the command line.

        Parameters:
        Returns:
        """
        # Prompts for calling input().
        prompt_name = ("\nEnter a bind variable name."
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
        bindvar_dict = dict()
        # One loop per bind variable name/value pair.
        while True:
            # Get bind variable name.
            bindvar_name = input(prompt_name).strip()
            if bindvar_name == '':
                # Done entering PL/SQL.
                break
            elif bindvar_name.upper() == 'Q':
                print('\nQuitting at your request.')
                self.cleanup()
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
                self.cleanup()
                exit(0)

            # Add bind variable name/value pair to dictionary of bind variables.
            bindvar_dict[bindvar_name] = bindvar_val

        # All done!
        self.bindvar_dict = bindvar_dict
        return

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
                self.cursor.execute(self.plsql, self.bindvar_dict)
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
        bindvar_dict_x = dict()

        # Execute the PL/SQL.
        self.set_plsql_text(plsql_x, bindvar_dict_x)
        tables_col_names, tables_rows, tables_row_count = self.run_plsql()

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
        plsql_x = """SELECT view_name, text AS view_sql
                     FROM user_views
                     ORDER BY view_name"""
        bindvar_dict_x = dict()

        # Execute the PL/SQL.
        self.set_plsql_text(plsql_x, bindvar_dict_x)
        views_col_names, views_rows, views_row_count = self.run_plsql()

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
        plsql_x = """
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

        # Execute the PL/SQL.
        bindvar_dict_x = {"table_name": table_name}
        self.set_plsql_text(plsql_x, bindvar_dict_x)
        columns_col_names, columns_rows, columns_row_count = self.run_plsql()

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
        plsql_x = """SELECT index_name, index_type, table_type, uniqueness
                     FROM user_indexes
                     WHERE table_name = :table_name
                     ORDER BY index_name"""
        bindvar_dict_x = {"table_name": table_name}

        # Execute the PL/SQL.
        self.set_plsql_text(plsql_x, bindvar_dict_x)
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
        plsql_x = """
        SELECT ic.column_position, column_name, descend, column_expression
        FROM user_ind_columns ic LEFT OUTER JOIN user_ind_expressions ie
        ON ic.column_position = ie.column_position
        AND ic.index_name = ie.index_name
        WHERE ic.index_name = :index_name
        ORDER BY ic.column_position"""
        bindvar_dict_x = {"index_name": index_name}

        # Execute the PL/SQL.
        self.set_plsql_text(plsql_x, bindvar_dict_x)
        ind_col_col_names, ind_col_rows, ind_col_row_count = self.run_plsql()

        # Return the information about this index.
        return ind_col_col_names, ind_col_rows

    def oracle_table_schema(self, colsep='|') -> None:
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
                prompt += str(index) + ': ' + table + '\n'
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
                    my_table = tables[choice]
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
        output_writerx.finish_up()
        return

    def oracle_view_schema(self, colsep='|') -> None:
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
                prompt += str(index) + ': ' + view_name + '\n'
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
        output_writerx.finish_up()
        return

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
    print()
    print_exception(*sys.exc_info(), limit=None, file=sys.stdout)
    return


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
    prompt = "\nEnter {}'s password: ".format(username)  # Accept anything.
    if sys.stdin.isatty():
        # getpass works in terminal windows, but hangs in PyCharm (to fix, do
        # "Edit configurations" & select "Emulate terminal in output console").
        password = getpass(prompt)
    else:
        # In Eclipse & IDLE, getpass uses "input", which echoes password.
        password = input(prompt)
    return password


def run_sqlplus(plsql: str) -> list:
    """ Run PL/SQL against database using sqlplus.

    Parameters:
        plsql (str): text of the PL/SQL to run.
    Returns:
        plsql_output (list): rows of output.
    """
    p = subprocess.Popen(['sqlplus', '/nolog'],  stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
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

    # Column separator for output.
    my_colsep = '|'

    # Text of commands to run in sqlplus.  Explanation of commands:
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
    plsql_for_sqlplus = """
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

    # Run above commands in sqlplus.
    sqlplus_output = run_sqlplus(plsql_for_sqlplus)

    # Show the output from running above commands in sqlplus.
    # Don't use write_rows, it'll crash because sqlplus output not all columnar.
    for line in sqlplus_output:
        print(line)

    # Set up to connect to Oracle instance specified above.
    database1 = OracleDB(username1, password1, hostname1, port_num1, instance1)

    # Connect to that Oracle instance.
    database1.open_connection()
    database1.print_all_connection_parameters()

    # Pass in database connection to my Oracle Client.
    my_oracle_client = OracleClient(database1)

    # See the Oracle table schema for my login.
    my_oracle_client.oracle_table_schema(colsep=my_colsep)
    print()

    # See the Oracle view schema for my login.
    my_oracle_client.oracle_view_schema(colsep=my_colsep)
    print()

    # Pass in text of PL/SQL and a dict of bind variables and their values.
    plsql1 = """SELECT actor, title, price, categoryname
               FROM products p INNER JOIN categories c
               ON p.category = c.category
               WHERE actor = :actor"""
    bindvar_dict1 = {'actor': 'CHEVY FOSTER'}
    my_oracle_client.set_plsql_text(plsql1, bindvar_dict1)

    # Execute the PL/SQL.
    col_names1, rows1, row_count1 = my_oracle_client.run_plsql()

    # Set up to write output.
    output_writer = OutputWriter(out_file_name='', align_col=True,
                                 col_sep='|')
    output_writer.get_align_col()
    output_writer.get_col_sep()
    output_writer.get_out_file_name()
    # Show the results.
    output_writer.write_rows(rows1, col_names1)

    # Clean up.
    output_writer.finish_up()
    col_names1 = None
    rows1 = None
    print()

    # From command line, read in PL/SQL & dict of bind variables & their values.
    my_oracle_client.get_plsql_text_from_command_line()
    my_oracle_client.get_bindvars_from_command_line()

    # Execute the PL/SQL.
    col_names2, rows2, row_count2 = my_oracle_client.run_plsql()

    # Set up to write output.
    output_writer = OutputWriter(out_file_name='', align_col=True,
                                 col_sep='|')
    output_writer.get_align_col()
    output_writer.get_col_sep()
    output_writer.get_out_file_name()
    # Show the results.
    output_writer.write_rows(rows2, col_names2)

    # Clean up.
    output_writer.finish_up()
    col_names2 = None
    rows2 = None
    print()
    database1.close_connection()
    database1.print_connection_status()
