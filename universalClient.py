""" universalClient.py

SUMMARY:
  Command-line universal database client.

REPOSITORY: https://github.com/DavidJLambert/Python-Universal-DB-Client

AUTHOR: David J. Lambert

VERSION: 0.2.8

DATE: Mar 2, 2020

PURPOSE:
  A sample of my Python coding, to demonstrate that I can write decent Python,
  test it, and document it.  I also demonstrate that I know relational
  databases and Linux.

DESCRIPTION:
  This is a command-line program that asks an end-user for SQL to execute on 1
  of 6 different relational databases, ordered by popularity as ranked in
  https://pypl.github.io/DB.html in Feb 2020:

  - Oracle
  - MySQL
  - Microsoft SQL Server
  - Microsoft Access
  - PostgreSQL
  - SQLite

  I also provide sample databases to run this program against (see below).

  The code for the 6 tested database types has been tested with CRUD statements
  (Create, Read, Update, Delete).  There is nothing to prevent the end-user
  from entering other SQL, such as ALTER DATABASE, CREATE VIEW, and BEGIN
  TRANSACTION, but none have been tested.

  A future version might include the ability to list databases, tables, views,
  indexes, and their fields without having to know the structure of any data
  dictionaries.  This is the easiest addition to make, so it is the most
  probable addition to this package.

PROGRAM REQUIREMENTS:

  + For connecting to Oracle, my code uses the cx_Oracle library, which is
    available on PyPI.  The cx_Oracle library requires the Oracle client
    libraries.  Several ways to obtain the Oracle client libraries are
    documented on https://cx-oracle.readthedocs.io/en/latest/installation.html.

    Cx_Oracle v7.3.0 supports Python versions 2.7 and 3.5-3.8,
    and Oracle client versions 11.2-19.

  + For connecting to MySQL, my code uses the pymysql library, which is
    available on PyPI.

    Pymysql v0.9.3 supports Python versions 2.7 and 3.4-3.8, plus MySQL and
    MariaDB versions 5.5 and newer.

  + For connecting to Microsoft SQL Server, my code used the pymssql library,
    which was available on PyPI, but the pymssql project has been discontinued.
    Instead, I use pyodbc.  Turbodbc has a reputation for being faster than
    pyodbc, but I got fatal errors when trying to install it.

  + For connecting to PostgreSQL, my code uses the psycopg2 library, which
    is available on PyPI.

    Psycopg2 v2.8.4 supports Python version 2.7 and 3.4-3.8, and
    PostgreSQL server versions 7.4-12.

  + For connecting to Microsoft Access, my code uses the pyodbc library,
    which is available on PyPI.  The pyodbc library requires the "Microsoft
    Access Database Engine 2016 Redistributable", which is available from
    https://www.microsoft.com/en-us/download/details.aspx?id=54920.

    Pyodbc v4.0.30 supports Python versions 2.7 and 3.4-3.8.

  + For connecting to SQLite, my code uses the sqlite3 library, part of the
    Python Standard Library.

    The sqlite3 library has been in the Standard Library since Python 2.5.

SAMPLE DATABASES TO TEST THIS PROGRAM ON:
  I provide 5 sample databases to run this program against, one for each of the
  5 types of tested database types listed in the previous section.  I have
  a test Oracle database on Windows, which is obviously not suitable for a
  freely downloadable sample database.

  Sample SQLite and Microsoft Access databases are included in this package in
  these locations:

  - databases/ds2.sqlite3
  - databases/ds2.accdb

  There are 3 VirtualBox Linux guests containing sample databases, one each for
  Microsoft SQL Server on Ubuntu (officially supported!), MySQL on Debian, and
  PostgreSQL on Debian.

  - MySQL:                https://1drv.ms/u/s!AieKzIY33GmRgcExQPbjBZ62X4tPCQ
  - Microsoft SQL Server: https://1drv.ms/u/s!AieKzIY33GmRgcQXIZ9mvqPNcEqHdw
  - PostgreSQL:           https://1drv.ms/u/s!AieKzIY33GmRgcEwOQinckQ9Buyk9w

  The sample databases all have the same data: the small version of the Dell
  DVD Store database, version 2.1, available at http://linux.dell.com/dvdstore.
  The data is in these tables:

  - CATEGORIES     --     16 records
  - CUSTOMERS      -- 20,000 records
  - CUST_HIST      -- 60,350 records
  - INVENTORY      -- 10,000 records
  - ORDERLINES     -- 60,350 records
  - ORDERS         -- 12,000 records
  - PRODUCTS       -- 10,000 records
  - REORDER        --      0 records
  - I've added table db_description, containing 1 record with my name and
    contact information.

  The MySQL sample database:
    - Available at https://1drv.ms/u/s!AieKzIY33GmRgcExQPbjBZ62X4tPCQ.
    - MySQL 5.5.60 on an Oracle VirtualBox virtual machine running Debian 8.11
      Jessie.  I've installed LXDE desktop 0.99.0-1 on it.
    - This virtual machine is based on a virtual machine created by Turnkey
      Linux (Turnkey GNU/Linux version 14.2), which is available at
      https://www.turnkeylinux.org/mysql.

  The Microsoft SQL Server sample database:
    - Available at https://1drv.ms/u/s!AieKzIY33GmRgcQXIZ9mvqPNcEqHdw.
    - Microsoft SQL Server 2017 Express Edition on an Oracle VirtualBox virtual
      machine running Ubuntu 16.04.3 server.  No desktop environment, command
      line only.
    - This virtual machine was installed from a Ubuntu 16.04.3 server iso image
      downloaded from https://www.ubuntu.com/download/server.

  The PostgreSQL sample database:
    - Available at https://1drv.ms/u/s!AieKzIY33GmRgcEwOQinckQ9Buyk9w.
    - PostgreSQL 9.4.19 on an Oracle VirtualBox virtual machine running Debian
      8.11 Jessie.  I've installed LXDE desktop 0.99.0-1 on it.
    - This virtual machine is based on a virtual machine created by Turnkey
      Linux (Turnkey GNU/Linux version 14.2), which is available at
      https://www.turnkeylinux.org/mysql.

  The Microsoft Access 2016 sample database:
    - Included in this package as databases/ds2.accdb.

  The SQLite sample database:
    - Included in this package as databases/ds2.sqlite3.
"""

# -------- IMPORTS

from __future__ import print_function
import os
import sys
from traceback import print_exception
from getpass import getpass
# Parent classes of all database exceptions (DB-API 2.0, see PEP 249).
from sqlite3 import DatabaseError, InterfaceError
import struct
import platform

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

if sys_version_info[0] == 2:
    # Python 2.x
    input = raw_input
    Dont_Catch = (Warning, StopIteration)
elif sys_version_info[1] <= 4:
    # Python 3.0-3.4
    Dont_Catch = (Warning, StopIteration)
else:
    # Python 3.5+
    Dont_Catch = (Warning, StopIteration, StopAsyncIteration)

# -------- CREATE AND INITIALIZE VARIABLES

db_port_class = 'db_port'
db_type_class = 'db_type'

# Database types.
oracle = 'oracle'
mysql = 'mysql'
sql_server = 'sql server'
postgresql = 'postgresql'
access = 'access'
sqlite = 'sqlite'
db_types = [oracle, mysql, sql_server, postgresql, access, sqlite]

map_type_to_lib = {oracle: 'cx_Oracle',
                   mysql: 'pymysql',
                   sql_server: 'pyodbc',
                   postgresql: 'psycopg2',
                   access: 'pyodbc',
                   sqlite: 'sqlite3'}

map_type_to_port = {oracle: 1521,
                    mysql: 3306,
                    sql_server: 1433,
                    postgresql: 5432,
                    access: None,
                    sqlite: None}

# All db types.
all_dbs = set(db_types)

# Db types in a file on the local machine.
db_local = {access, sqlite}

# Db types with a login.
db_has_login = all_dbs - db_local

# Db types with instances.
db_has_instance = db_has_login

# Db types that close cursors.
db_has_close = db_has_login

# Db types that can use a connection string in the connect() method.
db_uses_conn_str = all_dbs - {mysql}

ARRAY_SIZE = 20

# -------- CUSTOM CLASSES


class ExceptionUserQuit(Exception):
    """ End-user wants to quit this program. """
    pass


class ExceptionUserStartOver(Exception):
    """ End-user wants to start over entering database connection info. """
    pass


class ExceptionUserAnotherDB(Exception):
    """ Want to use another database now. """
    pass


class ExceptionUserNewSQL(Exception):
    """ Wnd-user wants to run different sql on the current connection. """
    pass


class ExceptionFatal(Exception):
    """ Quit because of a bug in this code. """
    pass

# -------- DEFINE FUNCTIONS


def main():
    """ Main loop.  Keep looping until break. """
    while True:

        db_type = ''
        cursor = None
        connection = None
        try:
            # Ask end-user for db type.
            db_type = ask_for_db_type()

            # Ask end-user for db location (either file path or hostname/port).
            db_host, db_port, db_path = ask_for_db_location(db_type)

            # Ask end-user for db instance, if this db type has them.
            db_instance = ask_for_db_instance(db_type)

            # Ask end-user for login (username, password) if db type has them.
            db_user, db_password = ask_for_db_login(db_type)

            # Connect to db (see if connection fails before asking for SQL).
            connection = connect_to_db(db_type, db_host, db_port, db_instance,
                                       db_path, db_user, db_password)

            while True:
                # Get SQL to execute.
                sql = ask_for_sql()
                try:
                    # Execute SQL.
                    cursor = run_sql(connection, sql)
                    # Print query results or db response.
                    print_response(cursor, sql)
                except ExceptionUserNewSQL:
                    # Just go to next sql statement.
                    pass
        except ExceptionFatal:
            # Sender already announced what the problem is.
            break
        except ExceptionUserQuit:
            print('\nQuitting at your request.')
            break
        except ExceptionUserStartOver:
            print('\nStarting over at your request.')
        except ExceptionUserAnotherDB:
            print('\nStarting another database at your request.')
        except Dont_Catch:
            # Catch and re-raise, so that later except clauses don't get these.
            raise
        except Exception:
            # Yes, I know, overly broad.  Just want to see stack trace.
            do_stacktrace()
            print('\nWill continue, if possible.')
        finally:
            disconnect_db(db_type, connection, cursor)
    return


def ask_for_db_type():
    """ Ask end-user for type of database (MySQL, PostgreSQL, etc.) to query.

    Args:
        none.
    Returns:
        db_type (string): the type of db to query (MySQL, PostgreSQL, etc.).
    Raises:
        none.
    """
    prompt = '\nEnter the number for your db type:'
    for i in range(len(db_types)):
        prompt += '\n({}) {}'.format(i+1, db_types[i])
    prompt += ', or\n(Q) to Quit program: '
    db_type = ask_and_check_int(prompt, msg='choice', name=db_type_class)
    return db_type


def ask_for_db_location(db_type):
    """ Ask the end-user for db location (either file path or hostname/port).

    Args:
        db_type (string): database type (MySQL, PostgreSQL, etc.).
    Returns:
        db_host (string): DNS name or IP address of database host.
        db_port (int): port number that database listens on.
        db_path (string): path of file containing database.
    Raises:
        none.
    """
    db_host = db_path = ''
    db_port = 0
    if db_type in db_local:
        prompt = ("\nEnter your db file's full path,"
                  '\n(Q) to Quit program, or'
                  '\n(S) to Start over: ')
        msg = "\n## '{}' is not a valid path. ##"
        db_path = ask_and_check_str(prompt, os.path.exists, msg, msg_arg=True)
    else:
        prompt = ("\nEnter the db server's host name or IP address,"
                  '\n(Q) to Quit program, or'
                  '\n(S) to Start over: ')
        msg = '\n## You did not enter a db host. ##'
        db_host = ask_and_check_str(prompt, echo, msg, msg_arg=False)

        default_port = '{} default is {}),'.format(db_type,
                                                   map_type_to_port[db_type])
        prompt = ('\nEnter the port (the ' + default_port +
                  '\n(Q) to Quit program, or' +
                  '\n(S) to Start over: ')
        db_port = ask_and_check_int(prompt, msg='port', name=db_port_class)
    return db_host, db_port, db_path


def ask_for_db_instance(db_type):
    """ Ask end-user for the db instance, if this db type has them.

    Args:
        db_type (string): database type (MySQL, PostgreSQL, etc.).
    Returns:
        db_instance (string): database instance.
    Raises:
        none.
    """
    db_instance = ''
    if db_type in db_has_instance:
        prompt = ('\nEnter the db instance,'
                  '\n(Q) to Quit program, or'
                  '\n(S) to Start over: ')
        msg = '\n## You did not enter a db instance. ##'
        db_instance = ask_and_check_str(prompt, echo, msg, msg_arg=False)
    return db_instance


def ask_for_db_login(db_type):
    """ Ask end-user for a db user name and password, if this db type has them.

    Args:
        db_type (string): database type (MySQL, PostgreSQL, etc.).
    Returns:
        db_user (string): username to log into db as.
        db_password (string): password for db_user.
    Raises:
        none.
    """
    db_user = db_password = ''
    if db_type in db_has_login:
        prompt = ('\nEnter the db user name,'
                  '\n(Q) to Quit program, or'
                  '\n(S) to Start over: ')
        msg = '\n## You did not enter a username. ##'
        db_user = ask_and_check_str(prompt, echo, msg, msg_arg=False)

        prompt = "\nEnter {}'s password: ".format(db_user)  # Accept anything.
        if sys.stdin.isatty():
            # Using terminal, OK to use getpass.
            db_password = getpass(prompt)
        else:
            # Use "input" (echoes password).  Getpass detects Eclipse & IDLE,
            # warns and uses "input", getpass doesn't detect PyCharm & hangs,
            # who knows what other environments do, just avoid the whole mess.
            db_password = ask_end_user(prompt)
    return db_user, db_password


def ask_for_sql():
    """ Ask end-user for SQL to execute.  No sanity checking done.

    Args:
        none.
    Returns:
        sql (string): SQL statement to execute.
    Raises:
        none.
    """
    prompt = ('\nEnter the SQL to execute in this db,'
              '\nReturn to finish entering the SQL,'
              '\n(Q) to Quit program, or'
              '\n(A) to use Another db: ')
    msg = '\n## You did not enter any SQL. ##'
    sql = ''
    func = echo
    while True:
        # Get the next line of SQL.
        response = ask_and_check_str(prompt, func, msg, msg_arg=False)
        # Use a prompt only for the first line of SQL.
        prompt = ''
        # Make no response OK for subsequent calls.
        func = lambda arg: "Spam!"

        if response == '':
            # Done entering SQL.
            break
        elif response.upper() == 'Q':
            raise ExceptionUserQuit()
        elif response.upper() == 'A':
            raise ExceptionUserAnotherDB()
        else:
            # Add more text to the current PL/SQL.
            sql += '\n' + response

    return sql


def connect_to_db(db_type, db_host, db_port, db_instance, db_path, db_user,
                  db_password):
    """ Connect to specified db.

    Args:
        db_type (string): the type of db to query (MySQL, PostgreSQL, etc.).
        db_host (string): DNS name or IP address of database host.
        db_port (int): port number that database listens on.
        db_instance (string): database instance.
        db_path (string): path of file containing database.
        db_user (string): username to log into db as.
        db_password (string): password for db_user.
    Returns:
        connection (connection): connection to database specified previously.
    Raises:
        ExceptionUserAnotherDB, NotImplementedError.
    """
    # Dynamic import of connection library for current db type.
    db_library = __import__(map_type_to_lib[db_type])

    if db_type == mysql:
        z = ''
    elif db_type == sql_server:
        z = 'DRIVER={{SQL Server}};UID={};PWD={};SERVER={};PORT={};DATABASE={}'
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
        raise ExceptionUserAnotherDB()

    if db_type in {sql_server, oracle, postgresql}:
        z = z.format(db_user, db_password, db_host, db_port, db_instance)
    elif db_type in db_local:
        z = z.format(db_path)

    if db_type in db_uses_conn_str:
        connection = db_library.connect(z)
    else:
        connection = db_library.connect(db_host, db_user, db_password,
                                        db_instance, db_port)
    return connection


def run_sql(connection, sql):
    """ Execute SQL on connection to previously specified database.

    Args:
        connection (connection): connection object.
        sql (string): SQL statement.
    Returns:
        cursor (cursor): cursor for specified connection.
    Raises:
        ExceptionUserNewSQL.
    """
    try:
        cursor = connection.cursor()
        cursor.execute(sql)
        return cursor
    except (DatabaseError, InterfaceError) as e:
        # Problem executing current SQL, so just get another SQL statement.
        print('\n'+str(e))
        raise ExceptionUserNewSQL()


def print_response(cursor, sql):
    """ Print the database's response to previous SQL statement.

    Args:
        cursor (cursor): cursor object.
        sql (string): SQL statement.
    Returns:
        none.
    Raises:
        none.
    """
    sql_type = sql.split()[0].upper()
    rowcount = cursor.rowcount

    if cursor is None:
        raise ExceptionFatal('Cursor is None.')
    elif sql_type == 'SELECT':
        # SQLite, Access, SQL Server: Rowcount always -1. Oracle: Rowcount
        # always 0.  PostgreSQL, MySQL: rowcount = number rows selected.
        if rowcount >= 0:
            print("\n## {} records were selected. ##".format(rowcount))

        # Get column names
        col_names = [item[0] for item in cursor.description]
        # Fetch and print rows in batches of ARRAY_SIZE.
        count = 0
        while True:
            # Fetch another batch of rows.
            some_rows = cursor.fetchmany(ARRAY_SIZE)
            count += len(some_rows)

            # Collect output.
            lines = [[item for item in row] for row in some_rows]

            # Find column widths that make columns straight & just wide enough.
            col_sizes = [len(col_name) for col_name in col_names]
            for line in lines:
                col_sizes = [max(size, len(str(col)))
                             for (size, col) in zip(col_sizes, line)]

            # Format and print the column names.
            formats = ['{{:^{}}}'.format(size) for size in col_sizes]
            col_names_fmt = ' '.join(formats)
            print('\n' + col_names_fmt.format(*col_names))

            # Print a separator between the column names and the query output.
            dashes = ['-'*col_size for col_size in col_sizes]
            print(' '.join(dashes))

            # Format and print the query output.
            formats = ['{{:{}}}'.format(size) for size in col_sizes]
            query_output_fmt = ' '.join(formats)
            [print(query_output_fmt.format(*line)) for line in lines]

            # If there's no more to print.
            if count == 0:
                print('\nNo rows.')
                break
            elif len(some_rows) < ARRAY_SIZE:
                print('\nNo more rows.')
                break

            # Maybe print more rows.
            prompt = ('\nHit Enter to see more rows,'
                      '\n(Q) to Quit program, or'
                      '\n(N) for No more rows: ')
            ask_end_user(prompt).upper()
    elif sql_type in ('INSERT', 'UPDATE', 'DELETE'):
        print('\n## {} rows affected. ##'.format(rowcount))
    else:  # Not a CRUD statement.  Have not thought about that situation.
        print('\n## {} rows affected. ##'.format(rowcount))
    return


def disconnect_db(db_type, connection, cursor):
    """ Close cursor on connection to db, then close connection to db.

    Args:
        db_type (string): database type.
        connection (connection): database connection handle.
        cursor (cursor): cursor handle.
    Returns:
        none.
    Raises:
        none.
    """
    condition = db_type in db_has_close
    if condition and cursor is not None:
        cursor.close()
    if condition and connection is not None:
        connection.close()
    return


def do_stacktrace():
    """ Print a stack trace, then resume program.

    Args:
        none.
    Returns:
        none.
    Raises:
        none.
    """
    print()
    print_exception(*sys.exc_info(), limit=None, file=sys.stdout)
    return


def ask_end_user(prompt):
    """ Only place for end-user input, except in ask_for_users_login (getpass).

    Args:
        prompt (string): prompt to present to end-user for input.
    Returns:
        response (string): response to prompt.  Always string, never integer.
    Raises:
        ExceptionUserQuit, ExceptionUserStartOver, ExceptionUserAnotherDB,
          ExceptionUserNewSQL.
    """
    response = input(prompt).strip()
    u_response = response.upper()
    if u_response == 'Q':
        raise ExceptionUserQuit()
    elif u_response == 'S':
        raise ExceptionUserStartOver()
    elif u_response == 'A':
        raise ExceptionUserAnotherDB()
    elif u_response == 'N':
        raise ExceptionUserNewSQL()
    return response


def echo(arg):
    """ Function that returns argument.

    Args:
        Any object.
    Returns:
        Argument.
    Raises:
        none.
    """
    return arg


def ask_and_check_str(prompt, test, msg, msg_arg):
    """ Ask end-user for string input, test input.  If test fails, ask again.

    Args:
        prompt (string): prompt to present to end-user for input.
        test (function): response must pass test.  test(response) == True
        msg (string): message to end-user if test fails.
        msg_arg (boolean): whether or not msg contains the end-user response.
    Returns:
        response (string): validated end-user response to "prompt" argument.
    Raises:
        none.
    """
    while True:
        response = ask_end_user(prompt)
        if test(response):
            break
        elif msg_arg:
            print(msg.format(response))
        else:
            print(msg)
    return response


def ask_and_check_int(prompt, msg, name):
    """ Ask end-user for integer input, test input.  If test fails, ask again.

    Args:
        prompt (string): prompt to present to end-user for input.
        msg (string): part of message to end-user if test fails.
        name (string): name of input sought.  For selecting functionality.
    Returns:
        response (int or str): validated end-user response to "prompt".
    Raises:
        none.
    """
    while True:
        response = ask_end_user(prompt)
        if not response:
            print('\n## You did not enter anything. ##')
        elif response.isdigit():
            response = int(response)
            if name == db_port_class:
                test = response in range(1, 65536)
            elif name == db_type_class:
                test = (response-1) in range(len(db_types))
            else:
                raise ValueError('ask_and_check_int: bad name {}.'.format(name))
            if test:
                if name == db_type_class:
                    response = db_types[response-1]
                    print('\n## Selected {}. ##'.format(response))
                break
            else:
                print("\n## '{}' is an invalid {}. ##".format(response, msg))
        else:
            print("\n## '{}' is an invalid {}. ##".format(response, msg))
    return response


# -------- IF __NAME__ ....


if __name__ == '__main__':
    main()
