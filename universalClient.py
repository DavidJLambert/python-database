""" universalClient.py

Universal Database Client
-------------------------

SUMMARY:
  Command-line universal database client.

VERSION:
  0.1.0

AUTHOR:
  David J. Lambert

DATE:
  September 19, 2018

PURPOSE:
  A sample of my Python coding, to demonstrate that I can write decent Python,
  test it, and document it.  I also demonstrate that I know relational
  databases and Linux.

DESCRIPTION:
  This is a command-line program that asks an end-user for SQL to execute on 1
  of 7 different relational databases:

  - Oracle
  - MySQL
  - Microsoft SQL Server
  - PostgreSQL
  - IBM DB2 (untested)
  - Microsoft Access 2016
  - SQLite

  I also provide sample databases to run this program against (see below).
  The code for DB2 is untested.

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
    Python versions 2.7 and 3.5-3.7 are supported by cx_Oracle v7.0.0.
    Oracle versions 11.2-18.3 are supported by cx_Oracle v7.0.0.
  + For connecting to MySQL, my code uses the pymysql library, which is
    available on PyPI.
    Python versions 2.7 and 3.4-3.7 are supported by pymysql v0.9.2.
    MySQL and MariaDB versions 5.5 and newer are supported by pymysql v0.9.2.
  + For connecting to Microsoft SQL Server, my code uses the pymssql library,
    which is available on PyPI.  The pymssql library requires Microsoft Visual
    C++ 14, which is available as "Microsoft Visual C++ Build Tools" on
    http://landinghub.visualstudio.com/visual-cpp-build-tools.
    Python versions 2.7 and 3.4-3.7 are supported by pymssql v2.1.4.
    Microsoft SQL Server versions 2005 and newer are supported by pymssql
    v2.1.4.
  + For connecting to PostgreSQL, my code uses psycopg2 library, which
    is available on PyPI.
    Python versions 2.6-2.7 and 3.2-3.6 are supported by psycopg2 v2.7.5.  I
    have no problems with using Python 3.7, even though it is unsupported.
    PostgreSQL server versions 7.4-10 are supported by psycopg2 v2.7.5.
  + For connecting to IBM DB2, my code uses the ibm_db library, which is
    available on PyPI.  The ibm_db library library requires Microsoft Visual
    C++ 14, which is available as "Microsoft Visual C++ Build Tools" on
    http://landinghub.visualstudio.com/visual-cpp-build-tools.
    I can not find which Python versions are supported by ibm_db v2.0.9, but I
    had no problems installing it in Python versions 2.7 and 3.3-3.7.
  + For connecting to Microsoft Access 2016, my code uses the pyodbc library,
    which is available on PyPI.  The pyodbc library requires the "Microsoft
    Access Database Engine 2016 Redistributable", which is available from
    https://www.microsoft.com/en-us/download/details.aspx?id=54920.
    Python versions 2.7 and 3.4-3.6 are supported by pyodbc v4.0.24.
  + For connecting to SQLite, my code uses the sqlite3 library, part of the
    Python Standard Library.
    The sqlite3 library has been in the Standard Library since Python 2.5.

SAMPLE DATABASES TO TEST THIS PROGRAM ON:
  I provide 5 sample databases to run this program against, one for each of the
  5 types of tested database types listed in the previous section.  I have
  ambitions of creating sample Oracle and DB2 databases.

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
    - Can be downloaded from https://1drv.ms/u/s!AieKzIY33GmRgcExQPbjBZ62X4tPCQ.
    - MySQL 5.5.60 on an Oracle VirtualBox virtual machine running Debian 8.11
      Jessie.  I've installed LXDE desktop 0.99.0-1 on it.
    - This virtual machine is based on a virtual machine created by Turnkey
      Linux (Turnkey GNU/Linux version 14.2), which is available at
      https://www.turnkeylinux.org/mysql.

  The Microsoft SQL Server sample database:
    - Can be downloaded from https://1drv.ms/u/s!AieKzIY33GmRgcQXIZ9mvqPNcEqHdw.
    - Microsoft SQL Server 2017 Express Edition on an Oracle VirtualBox virtual
      machine running Ubuntu 16.04.3 server.  No desktop environment, command
      line only.
    - This virtual machine was installed from a Ubuntu 16.04.3 server iso image
      downloaded from https://www.ubuntu.com/download/server.

  The PostgreSQL sample database:
    - Can be downloaded from https://1drv.ms/u/s!AieKzIY33GmRgcEwOQinckQ9Buyk9w.
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
import os.path
import sys
import traceback
import getpass

# -------- PYTHON VERSION STUFF

if sys.version_info[0] == 2:
    input = raw_input
    FileNotFoundError = IOError

# -------- CREATE AND INITIALIZE VARIABLES

# Supported relational db types, in descending order in popularity.
default_ports = [1521, 3306, 1433, 5432, 50000, 0, 0]
db_types = ['oracle', 'mysql', 'sql server', 'postgresql', 'db2', 'access',
            'sqlite']
connection_libs = ['cx_Oracle', 'pymysql', 'pymssql', 'psycopg2', 'ibm_db',
                   'pyodbc', 'sqlite3']

map_type_to_lib = dict(zip(db_types, connection_libs))
map_type_to_port = dict(zip(db_types, default_ports))

# All db types.
all_dbs = set(db_types)

# Db types in a file on the local machine.
db_local = {'access', 'sqlite'}

# Db types with a login.
db_has_login = all_dbs - db_local

# Db types with instances.
db_has_instance = db_has_login

# Db types that close cursors.
db_has_close = db_has_login

# Db types that can use a connection string in the connect() method.
db_uses_conn_str = ['access', 'sqlite', 'oracle', 'postgresql', 'db2']

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
            print("\nQuitting at your request.")
            break
        except ExceptionUserStartOver:
            print("\nStarting over at your request.")
        except ExceptionUserAnotherDB:
            pass
        except Exception:
            # Yes, I know, overly broad.  Just want to see stack trace.
            do_stacktrace()
            print("\nWill continue, if possible.")
        finally:
            disconnect_db(db_type, connection, cursor)


def ask_for_db_type():
    """ Ask end-user for type of database (MySQL, PostgreSQL, etc.) to query.

    Args:
        none.
    Returns:
        db_type (string): the type of db to query (MySQL, PostgreSQL, etc.).
    Raises:
        none.
    """
    prompt = "\nEnter the number for your db type:"
    for i in range(7):
        prompt += "\n(%d) %s" % (i+1, db_types[i])
    prompt += ", or\n(Q) to Quit program: "
    while True:
        key = ask_end_user(prompt)
        if not key:
            print("\n## You did not enter anything. ##")
        elif key.isdigit():
            key = int(key)
            if key-1 in range(len(db_types)):
                db_type = db_types[key-1]
                print("\n## Selected %s. ##" % db_type)
                break
            else:
                print("\n## '%d' is an invalid choice. ##" % key)
        else:
            print("\n## '%s' is an invalid choice. ##" % key)
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
    prompt = ("\nEnter your db file's full path," +
              "\n(Q) to Quit program, or" +
              "\n(S) to Start over: ")
    if db_type in db_local:
        while True:
            db_path = ask_end_user(prompt)
            if os.path.exists(db_path):
                break
            else:
                print("\n## '%s' is not a valid path. ##" % db_path)
    else:
        prompt = ("\nEnter the db server's host name or IP address," +
                  "\n(Q) to Quit program, or" +
                  "\n(S) to Start over: ")
        while True:
            db_host = ask_end_user(prompt)
            if db_host:
                break
            else:
                print("\n## You did not enter a db host. ##")

        default = "%s default is %d)," % (db_type, map_type_to_port[db_type])
        prompt = ("\nEnter the port (the " + default +
                  "\n(Q) to Quit program, or" +
                  "\n(S) to Start over: ")
        while True:
            db_port = ask_end_user(prompt)
            if not db_port:
                print("\n## You did not enter a port. ##")
            elif db_port.isdigit():
                db_port = int(db_port)
                if db_port in range(1, 65536):
                    break
                else:
                    print("\n## '%d' is an invalid port. ##" % db_port)
            else:
                print("\n## '%s' is an invalid port. ##" % db_port)
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
        prompt = ("\nEnter the db instance," +
                  "\n(Q) to Quit program, or" +
                  "\n(S) to Start over: ")
        while True:
            db_instance = ask_end_user(prompt)
            if db_instance:
                break
            else:
                print("\n## You did not enter a db instance. ##")
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
        prompt = ("\nEnter the db user name," +
                  "\n(Q) to Quit program, or" +
                  "\n(S) to Start over: ")
        while True:
            db_user = ask_end_user(prompt)
            if db_user:
                break
            else:
                print("\n## You did not enter a username. ##")
        prompt = "\nEnter %s's password: " % db_user  # Accept anything.
        db_password = getpass.getpass(prompt=prompt)  # Doesn't work in PyCharm!
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
    prompt = ("\nEnter the SQL to execute in this db," +
              "\n(Q) to Quit program, or" +
              "\n(A) to use Another db: ")
    while True:
        sql = ask_end_user(prompt)
        if sql:
            break
        else:
            print("\n## You did not enter any SQL. ##")
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

    conn_str = ''
    if db_type in ("mysql", "sql server"):
        pass
    elif db_type == "oracle":
        conn_str = ("%s/%s@%s:%d/%s" %
                    (db_user, db_password, db_host, db_port, db_instance))
    elif db_type == "postgresql":
        conn_str = ("host='%s' dbname='%s' user='%s' password='%s' port='%d'"
                    % (db_host, db_instance, db_user, db_password, db_port))
    elif db_type == "db2":
        conn_str = ("DATABASE=%s;HOSTNAME=%s;PORT=%s;PROTOCOL=%s;UID=%s;PWD=%s;"
                    % (db_instance, db_host, db_port, 'TCPIP', db_user,
                       db_password))
    elif db_type == "access":
        conn_str = (r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=%s;"
                    % db_path)
    elif db_type == "sqlite":
        conn_str = db_path
    else:
        print("Unknown db type %s, aborting." % db_type)
        raise ExceptionUserAnotherDB()

    if db_type in db_uses_conn_str:
        connection = db_library.connect(conn_str)
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
    except Exception as e:
        # Problem executing current SQL, so just get another SQL statement.
        # TODO improve exception handling
        print()
        print(e)
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
        raise ExceptionFatal("Cursor is None.")
    elif sql_type in ("INSERT", "UPDATE", "DELETE"):
        print("\n## %d rows affected. ##" % rowcount)
    elif sql_type == "SELECT":
        # Rowcount useless in selects, always -1: sqlite, access
        # Rowcount = total number rows selected: postgresql
        rowcount = cursor.rowcount
        if rowcount >= 0:
            print("\n## A total of %d records were selected. ##" % rowcount)
        # Print row of column names
        fields = []
        for row in cursor.description:
            fields.append(row[0])
        print("\nColumns:\t" + "\t".join(fields))

        # Fetch and print rows in batches of ARRAY_SIZE.
        count = 0
        while True:
            # Fetch another batch of rows.
            some_rows = cursor.fetchmany(ARRAY_SIZE)
            if len(some_rows) == 0 or some_rows is None:
                if count == 0:
                    print("\nNo rows.")
                else:
                    print("\nNo more rows.")
                break
            # Now print this batch of rows.
            for row in some_rows:
                count += 1
                iter_ = [str(item) for item in row]
                print("Row %d:\t" % count + "\t".join(iter_))
            if len(some_rows) < ARRAY_SIZE:
                print("\nNo more rows.")
                break
            # Maybe print another batch.
            prompt = ("\nHit Enter to see rows %d-%d," % (count+1,
                      count+ARRAY_SIZE) +
                      "\n(Q) to Quit program, or" +
                      "\n(N) for No more rows: ")
            ask_end_user(prompt).upper()
    else:  # Not a CRUD statement.  Have not thought about that situation.
        print("\n## %d rows affected. ##" % rowcount)

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
    exc_type, exc_value, exc_traceback = sys.exc_info()
    traceback.print_exception(exc_type, exc_value, exc_traceback, limit=None,
                              file=sys.stdout)
    return


def ask_end_user(prompt):
    """ Only place for end-user input, except in ask_for_users_login (getpass).

    Args:
        prompt (string): prompt passed from calling function.
    Returns:
        response (string): response to prompt.  Always string, never integer.
    Raises:
        ExceptionUserQuit, ExceptionUserStartOver, ExceptionUserAnotherDB,
          ExceptionUserNewSQL.
    """
    response = input(prompt).strip()
    u_response = response.upper()
    if u_response == "Q":
        raise ExceptionUserQuit()
    elif u_response == "S":
        raise ExceptionUserStartOver()
    elif u_response == "A":
        raise ExceptionUserAnotherDB()
    elif u_response == "N":
        raise ExceptionUserNewSQL()

    return response

# -------- IF __NAME__ ....


if __name__ == "__main__":
    main()
