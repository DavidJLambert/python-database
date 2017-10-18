""" Sample.py
Summary: Command-line universal database client.
Version: 1.0
Author: David Joel Lambert
Date: October 17, 2017

Purpose:
    A sample of the author's Python coding, to denomstrate that he can write
decent Python, and that he knows relational databases.

Description:
    This is a command-line program that asks an end-user for SQL to execute on
one of five different relational databases: Microsoft SQL Server, PostgreSQL,
MySQL, Microsoft Access, and SQLite. The code contains complete Oracle hooks,
but no sample Oracle database exists (at the moment).  The author also has
ambitions of adding code for DB2 and adding a DB2 sample database.
    Bundled with this code are 2 VirtualBox Linux guests containing sample
databases, one each for MySQL and PostgreSQL.  Sample Microsoft Access and
SQLite databases are also included.  The sample databases all contain the same
data: the small version of the Dell DVD Store database, version 2.1.
    The author has a virtual machine with Microsoft SQL Server on it, but it
can not be shared because of obvious Windows licensing issues.  The upcoming
SQL Server 2017 runs on Linux (surprising but smart move!), and the author may
migrate the existing SQL Server sample to it and bundle it.
    The code for the 5 implemented databases has been tested with CRUD
statements.  There is nothing to prevent the end-user from entering other SQL,
such as ALTER DATABASE, CREATE VIEW, and BEGIN TRANSACTION, but none have been
tested.
    A future version might include the ability to list databases, tables,
views, indexes, and their fields without having to know the structure of any
data dictionaries.  This is the easiest addition to make, so it is the most
probable addition to this package.
"""

########## IMPORTS

import os.path
import errno
import sys
import traceback
import getpass
import collections

########## CREATE AND INITIALIZE VARIABLES

# Supported relational db types, in descending order in popularity.
db_types        = [   "oracle",     "mysql", "ms sql server", "postgresql",
                         "db2", "ms access",        "sqlite"]
db_keys         = [        "A",         "B",             "C",          "D",
                           "E",         "F",             "G"]
default_ports   = [       1521,        3306,            1433,         5432,
                         50000,        None,            None]
connection_libs = ["cx_Oracle",   "pymysql",       "pymssql",   "psycopg2",
                      "ibm_db",    "pyodbc",       "sqlite3"]

# ordered so db_keys listed alphabetically in ask_for_db_type.
map_key_to_type  = collections.OrderedDict()
map_type_to_port = dict()
map_type_to_lib  = dict()
for i in range( len(db_types) ):
    map_key_to_type [  db_keys[i] ] = db_types[i]
    map_type_to_port[ db_types[i] ] = default_ports[i]
    map_type_to_lib [ db_types[i] ] = connection_libs[i]

all_dbs          = set(db_types)

# Db types in a file on the local machine.
local_dbs        = set(["ms access", "sqlite"])

# Db types with a login.
db_has_logins    = all_dbs - local_dbs

# Db types with instances.
db_has_instances = db_has_logins

# Db types that close cursors.
db_has_close     = db_has_logins

# DB types that can use a connection string in the connect() method.
db_uses_conn_str = ["ms access", "sqlite", "oracle", "postgresql", "db2"]

array_size = 20

########## DEFINE CUSTOM CLASSES

# If end-user wants to quit this program.
class Exception_UserQuit(Exception):
    """ Raised when end user wants to quit this program. """
    pass

# If end-user wants to abandon partially entered db specs and start over.
class Exception_UserStartOver(Exception):
    """ Raised when end user wants to start over the process of entering
        database connection info. """
    pass

# Want another db to work with.
class Exception_UserAnotherDB(Exception):
    """ The same as Exception_UserStartOver(Exception). """
    pass

# Want another SQL statement on current connection.
class Exception_UserNewSQL(Exception):
    """ Raised when end user wants to start over process of entering SQL. """
    pass

# Program bug detected.
class Exception_Fatal(Exception):
    """ Quit because of a bug in this code. """
    pass

########## DEFINE FUNCTIONS

# Main loop.  Keep looping until break.
def main():
    """ The high-level code for the universal database client."""
    while True:

        db_type    = None
        connection = None
        cursor     = None
        try:
            # Ask end-user for db type.
            db_type = ask_for_db_type()

            # Ask end-user for db location (either file path or hostname/port).
            db_location = ask_for_db_location( db_type )

            # Ask end-user for db instance if this db type has them.
            db_instance = ask_for_db_instance( db_type )

            # Ask end-user for login (username, password) if this db type has them.
            db_login = ask_for_db_login( db_type )

            # Connect to db (see if connection fails before asking for SQL).
            connection = connect_to_db(db_type, db_location, db_instance, db_login)

            while True:
                # Get SQL to execute.
                SQL = ask_for_sql()
                try:
                    # Execute SQL.
                    cursor = run_sql( connection, SQL )
                    # Print query results or db response.
                    print_response( cursor, SQL )
                except Exception_UserNewSQL as e:
                    # Just go to next SQL statement.
                    pass
        except Exception_Fatal as e:
            # Sender already announced what the problem is.
            break
        except Exception_UserQuit as e:
            print("\nQuitting at your request.")
            break
        except Exception_UserStartOver as e:
            print("\nStarting over at your request.")
        except Exception_UserAnotherDB as e:
            pass
        except Exception as e:
            do_stacktrace()
            print("\nWill continue, if possible.")
        finally:
            disconnect_db( db_type, connection, cursor )

def ask_for_db_type():
    """ Ask end-user for type of database (MySQL, PostgreSQL, etc.) to query.

    Args:
        none
    Returns:
        db_type (string): the type of db to query (MySQL, PostgreSQL, etc.).
    Raises:
        none
    """
    while True:
        prompt = "\nEnter the letter for your db type:"
        for key in db_keys:
            prompt += "\n(%s) %s" % (key, map_key_to_type[key])
        prompt +=     ", or\n(Q) to Quit program: "
        key = ask_end_user( prompt ).upper()
        if key in db_keys:
            db_type = map_key_to_type[key]
            print("Selected %s." % (db_type))
            break
        else:
            print( key + " is an invalid choice.")
    return db_type

def ask_for_db_location( db_type ):
    """ Ask the end-user for db location (either file path or hostname/port).

    Args:
        param1 (string): db_type, database type (MySQL, PostgreSQL, etc.)
    Returns:
        db_location (string): file path of db
          OR
        db_location ([string,string]): [hostname, port] of database server.
    Raises:
        none
    """
    if db_type in local_dbs:
        db_location = ask_end_user("\nEnter your db file's full path,"
                                   "\n(Q) to Quit program, or"
                                   "\n(S) to Start over: ")
        validate_file_path( db_location )
    else:
        db_host = ask_end_user("\nEnter host name or IP address of db server,"
                               "\n(Q) to Quit program, or"
                               "\n(S) to Start over: ")
        stuff = "%s default is %d)," % ( db_type, map_type_to_port[db_type] )
        prompt = ( "\nEnter the port (the " + stuff +
                   "\n(Q) to Quit program, or" +
                   "\n(S) to Start over: " )
        db_port = ask_end_user(prompt)
        db_port = int(db_port)
        db_location = [ db_host, db_port ]
    return db_location

def validate_file_path( db_location ):
    """ Check that the file path points to a file.

    Args:
        param1 (string): db_location, file path of database.
    Returns:
        none
    Raises:
        FileNotFoundError.
    """
    if not os.path.exists(db_location):
        raise FileNotFoundError( errno.ENOENT, os.strerror(errno.ENOENT),
                                 db_location )
    return

def ask_for_db_instance( db_type ):
    """ Ask end-user for the db instance, if this db type has them.

    Args:
        param1 (string): db_type, database type (MySQL, PostgreSQL, etc.)
    Returns:
        db_instance (string): database instance.
    Raises:
        none
    """
    if db_type in db_has_instances:
        return ask_end_user("\nEnter the db instance,"
                            "\n(Q) to Quit program, or"
                            "\n(S) to Start over: " )
    return None

def ask_for_db_login( db_type ):
    """ Ask end-user for a db user name and password, if this db type has them.

    Args:
        param1 (string): db_type, database type (MySQL, PostgreSQL, etc.)
    Returns:
        db_login ([string,string]): [username, password].
    Raises:
        none
    """
    if db_type in db_has_logins:
        db_user = ask_end_user("\nEnter the db user name,"
                              "\n(Q) to Quit program, or\n(S) to Start over: ")
        db_password = getpass.getpass("\nEnter %s's password: " % (db_user) )
        return [ db_user, db_password ]
    return [ None, None ]

def ask_for_sql():
    """ Ask end-user for SQL to execute.  No sanity checking done.

    Args:
        none
    Returns:
        SQL (string): SQL statement to execute.
    Raises:
        none
    """
    return ask_end_user("\nEnter SQL to execute in this db,"
                        "\n(Q) to Quit program, or"
                        "\n(A) to use Another db: ")

#  Connect to db before SQL requested.  Otherwise, end user may enter SQL and
#  then get failure to connect to database, wasting effort to enter SQL.
def connect_to_db(db_type, db_location, db_instance, db_login):
    """ Connect to specified db.

    Args:
        param1 (string): db_type, database type.
        param2 (string): database location, either file path or hostname/port.
    Returns:
        connection (object): connection to database specified previously.
    Raises:
        Exception_UserAnotherDB, NotImplementedError.
    """
    db_user, db_password = db_login
    if db_type not in local_dbs:
        db_host, db_port = db_location
        # Otherwise db_location is a file path

    library = map_type_to_lib[db_type]
    exec("import  "      + library, globals(), globals())
    exec("db_library = " + library, globals(), globals())

    if   db_type == "oracle":
        raise NotImplementedError("Oracle is not implemented yet.")
        conn_str = ( "%s/%s@%s:%d/%s" %
                     (db_user, db_password, db_host, db_port, db_instance) )
    elif db_type in ["mysql", "ms sql server"]:
        pass
    elif db_type == "postgresql":
        conn_str = ( "host='%s' dbname='%s' user='%s' password='%s' port='%d'"
                     % (db_host, db_instance, db_user, db_password, db_port) )
    elif db_type == "db2":
        raise NotImplementedError("DB2 is not implemented yet.")
        conn_str = ( "DATABASE=" + db_instance + ";HOSTNAME=" + db_host +
                     ";UID=" + db_user + ";PWD=" + db_password + ";" )
    elif db_type == "ms access":
        conn_str = ( r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)}' +
                     r';DBQ=%s;' % (db_location) )
    elif db_type == "sqlite":
        conn_str = db_location
    else:
        print("Unknown db type %s, aborting." % (db_type))
        raise Exception_UserAnotherDB()

    if db_type in db_uses_conn_str:
        connection = db_library.connect(conn_str)
    else:
        connection = db_library.connect(db_host, db_user, db_password,
                                        db_instance, db_port)

    return connection

def run_sql( connection, SQL ):
    """ Execute SQL on connection to previously specified database.

    Args:
        param1 (object): connection.
        param2 (string): SQL statement.
    Returns:
        cursor (object): cursor for specified connection.
    Raises:
        Exception_UserNewSQL.
    """
    try:
        cursor = connection.cursor()
        cursor.execute(SQL)
        return cursor
    except Exception as e:
        # Problem executing current SQL, so just get another SQL statement.
        print()
        print(e)
        raise Exception_UserNewSQL()
    return

def print_response( cursor, SQL ):
    """ Print the database's response to previous SQL statement.

    Args:
        param1 (object): cursor.
        param2 (string): SQL statement.
    Returns:
        none
    Raises:
        none.
    """
    sql_type = SQL.split()[0].upper()
    rowcount = cursor.rowcount

    if cursor is None:
        raise Exception_Fatal("Cursor is None.")
    elif sql_type in ("INSERT", "UPDATE", "DELETE"):
        print("%d rows affected." % (rowcount))
    elif sql_type == "SELECT":
        # Rowcount useless in selects, always -1: sqlite, access
        # Rowcount = total number rows selected: postgresql
        if cursor.rowcount >= 0:
            print("A total of %d records were selected.")
        # Print row of column names
        fields = []
        for row in cursor.description:
            fields.append(row[0])
        print("\nColumns:\t" + "\t".join(fields))

        # Fetch and print rows in batches of array_size.
        count = 0
        while True:
            # Fetch another batch of rows.
            some_rows = cursor.fetchmany(array_size)
            if len(some_rows) == 0 or some_rows is None:
                if count == 0:
                    print("\nNo rows.")
                else:
                    print("\nNo more rows.")
                break
            # Now print this batch of rows.
            for row in some_rows:
                count += 1
                iter = [str(item) for item in row]
                print( "Row %d:\t" % (count) + "\t".join(iter) )
            if len(some_rows) < array_size:
                print("\nNo more rows.")
                break
            # Maybe print another batch.
            prompt = ( "\nHit Enter to see rows %d-%d," % (count+1,
                       count+array_size) + 
                       "\n(Q) to Quit, or" +
                       "\n(N) for No more rows: " )
            try:
                more_rows = ask_end_user( prompt ).upper()
            except Exception_UserNewSQL as e:
                raise Exception_UserNewSQL()
                break
    else: # Not a CRUD statement.  Have not thought about that situation.
        print("%d rows affected." % (rowcount))

    return

# Close cursor on connection to db, then close connection to db.
def disconnect_db( db_type, connection, cursor ):
    """ Disconnect from db.

    Args:
        param1 (string): database type.
        param2 (object): connection.
        param3 (object): cursor.
    Returns:
        none
    Raises:
        none
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
        none
    Returns:
        none
    Raises:
        none
    """
    print()
    exc_type, exc_value, exc_traceback = sys.exc_info()
    traceback.print_exception(exc_type, exc_value, exc_traceback, limit=None,
                              file=sys.stdout)
    return

def ask_end_user( prompt ):
    """ Only place for end-user input, except in ask_for_users_login (getpass).

    Args:
        param1 (string): prompt passed from calling function.
    Returns:
        response (string): response to prompt.
    Raises:
        Exception_UserQuit, Exception_UserStartOver, Exception_UserAnotherDB,
          Exception_UserNewSQL.
    """
    response = input(prompt).strip()
    uresponse = response.upper()
    if   uresponse == "Q":
        raise Exception_UserQuit()
    elif uresponse == "S":
        raise Exception_UserStartOver()
    elif uresponse == "A":
        raise Exception_UserAnotherDB()
    elif uresponse == "N":
        raise Exception_UserNewSQL()

    return response

########## MAIN

main()

########## IF __NAME__ ....

if __name__ == "__main__":
    pass
