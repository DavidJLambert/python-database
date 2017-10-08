########## IMPORTS

from collections import OrderedDict
import os.path
import errno
import sys
import traceback
import getpass
#import sqlparse

########## CREATE AND INITIALIZE VARIABLES

# Supported relational db types, in descending order in popularity.
db_types        = [   "oracle",   "mysql", "ms sql server", "postgresql",    "db2", "ms access",  "sqlite"]
db_keys         = [        "A",       "B",             "C",          "D",      "E",         "F",       "G"]
default_ports   = [       1521,      3306,            1433,         5432,    50000,        None,      None]
connection_libs = ["cx_Oracle", "pymysql",       "pymssql",   "psycopg2", "ibm_db",    "pyodbc", "sqlite3"]

map_key_to_type  = OrderedDict() # ordered so db_keys listed alphabetically in ask_for_db_type
map_type_to_port = dict()
map_type_to_lib  = dict()
for i in range( len(db_types) ):
    map_key_to_type [ db_keys[i]  ] = db_types[i]
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

# DB types that have BEGIN TRANS.
db_has_begin     = set() # @@@@@@@@@@@@@@@@@@@@@@@@@@@@

# DB types that have COMMIT or COMMIT TRANS.
db_has_commit    = set() # @@@@@@@@@@@@@@@@@@@@@@@@@@@@

# access: no transactions, no explain
# sqlite: BEGIN TRANSACTION, COMMIT TRANSACTION, END TRANSACTION, ROLLBACK TRANSACTION, SAVEPOINT, RELEASE SAVEPOINT, EXPLAIN

# DB types that have EXPLAIN PLAN.
db_has_explain   = set() # @@@@@@@@@@@@@@@@@@@@@@@@@@@@

# DB types that can use a connection string in the connect() method.
db_uses_conn_str = ["ms access", "sqlite", "oracle", "postgresql", "db2"]

array_size = 20

########## DEFINE CUSTOM CLASSES

# If end-user wants to quit this program.
class Exception_Quit(Exception):
    pass

# If end-user wants to abandon partially entered db specs and start over.
class Exception_StartOver(Exception):
    pass

# Want new db to work with.
class Exception_NewDB(Exception):
    pass

# Problem executing SQL.
class Exception_BadSQL(Exception):
    pass

# Program bug detected.
class Exception_Fatal(Exception):
    pass

########## DEFINE FUNCTIONS

def ask_for_db_type():
    # Ask end-user for db type.
    while True:
        prompt = "\nEnter the letter for your db type:"
        for key in db_keys:
            prompt += "\n(%s) %s" % (key, map_key_to_type[key])
        prompt +=     ", or\n(Q) to Quit program: "
        key = ask_end_user( prompt ).upper() # Q/Exception_Quit
        if key in db_keys:
            db_type = map_key_to_type[key]
            print("Selected %s." % (db_type))
            break
        else:
            print( key + " is an invalid choice.")
    return db_type

def ask_for_db_location( db_type ):
    # Ask end-user for db location (either file path or hostname/port).
    if db_type in local_dbs:
        db_location = ask_end_user("\nEnter your db file's full path,"
                                   "\n(Q) to Quit program, or\n(S) to Start over: ") # Q/Exception_Quit, S/Exception_StartOver
        validate_file_path( db_location )
    else:
        db_host = ask_end_user("\nEnter host name or IP address of db server," # Q/Exception_Quit, S/Exception_StartOver
                               "\n(Q) to Quit program, or\n(S) to Start over: ")
        db_port = ask_end_user("\nEnter the port (the " + \
                               "%s default is %d)," % ( db_type, map_type_to_port[db_type] ) + \
                               "\n(Q) to Quit program, or\n(S) to Start over: " ) # Q/Exception_Quit, S/Exception_StartOver
        db_port = int(db_port)
        db_location = [ db_host, db_port ]
    return db_location

def validate_file_path( db_location ):
    if not os.path.exists(db_location):
        raise FileNotFoundError( errno.ENOENT, os.strerror(errno.ENOENT), db_location )
    return

def ask_for_db_instance( db_type ):
    # Ask end-user for db instance if this db type has them.
    if db_type in db_has_instances:
        return ask_end_user("\nEnter the db instance,"
                            "\n(Q) to Quit program, or\n(S) to Start over: " ) # Q/Exception_Quit, S/Exception_StartOver
    return None

def ask_for_db_login( db_type ):
    # Ask end-user for login (username, password) if this db type has them.
    if db_type in db_has_logins:
        db_user = ask_end_user("\nEnter the db user name,"
                               "\n(Q) to Quit program, or\n(S) to Start over: ") # Q/Exception_Quit, S/Exception_StartOver
        db_password = getpass.getpass("\nEnter %s's password: " % (db_user) )
        return [ db_user, db_password ]
    return [ None, None ]

def ask_for_sql():
    # Ask end-user for SQL.  No sanity checking.
    # Bind variables?????  # @@@@@@@@@@@@@@@@@@@@@@@@@@@@
    # LIST TABLES, VIEWS, INDEXES, COLUMNS. DATABASES
    return ask_end_user("\nEnter SQL to execute in this db,"
                        "\n(Q) to Quit program, or\n(N) to use a New db: ") # Q/Exception_Quit, N/Exception_NewDB

def connect_to_db(db_type, db_location, db_instance, db_login):
    # Connect to db (see if connection fails before asking for SQL).

    db_user, db_password = db_login
    if db_type not in local_dbs:
        db_host, db_port = db_location
        # Otherwise db_location is a file path

    library = map_type_to_lib[db_type]
    exec("import  "      + library, globals(), globals())
    exec("db_library = " + library, globals(), globals())

    if   db_type == "oracle":
        raise NotImplementedError("Oracle is not implemented yet.")
        conn_str = "%s/%s@%s:%d/%s" % (db_user, db_password, db_host, db_port, db_instance)
    elif db_type in ["mysql", "ms sql server"]:
        pass
    elif db_type == "postgresql":
        conn_str = "host='%s' dbname='%s' user='%s' password='%s' port='%d'" % (db_host, db_instance, db_user, db_password, db_port)
    elif db_type == "db2":
        raise NotImplementedError("DB2 is not implemented yet.")
        conn_str = "DATABASE=" + db_instance + ";HOSTNAME=" + db_host + ";UID=" + db_user + ";PWD=" + db_password + ";"
    elif db_type == "ms access":
        conn_str = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=%s;' % (db_location)
    elif db_type == "sqlite":
        conn_str = db_location
    else:
        print("Unknown db type %s, aborting." % (db_type))
        raise Exception_NewDB() # N/Exception_NewDB

    if db_type in db_uses_conn_str:
        connection = db_library.connect(conn_str)
    else:
        connection = db_library.connect(db_host, db_user, db_password, db_instance, db_port)

    return connection

def run_sql( connection, SQL ):
    # Execute SQL
    # Could use sqlparse and turn fixed values into bind variables # @@@@@@@@@@@@@@@@@@@@@@@@@@@@
    # UNSAFE!!
    # # What happens if id = "1'; DROP DATABASE somedb" ?
    # delstatmt = "DELETE FROM `maillist_subscription` WHERE id = '%s'" % (id,)
    # cursor.execute(delstatmt)
    # conn.commit() # @@@@@@@@@@@@@@@@@@@@@@@@@@@@
    # SAFE!
    # delstatmt = "DELETE FROM `maillist_subscription` WHERE id = ?"
    # cursor.execute(delstatmt, (id,))
    # conn.commit()
    try:
        cursor = connection.cursor()
        cursor.execute(SQL)
        #cursor.execute("select * from CUSTOMERS where age = (?)",(54,)) # @@@@@@@@@@@@@@@@@@@@@@@@@@@@
        return cursor
    except Exception as e:
        print()
        print(e)
        raise Exception_BadSQL
    return

def print_response( cursor, SQL ):
    # Print query results or db response.

    sql_type = SQL.split()[0].upper()

    if cursor is None:
        print("Cursor is None.")
    elif True:
    #elif sql_type == "SELECT": # @@@@@@@@@@@@@@@@@@@@@@@@@@@@
        # Print row of column names
        fields = []
        for row in cursor.description: 
            fields.append(row[0])
        print("\nColumns:\t" + "\t".join(fields))
        # Fetch and print rows in batches of array_size.
        count = 0
        while True:
            # Fetch the next batch of rows.
            some_rows = cursor.fetchmany(array_size)
            # Now print this batch of rows.
            if len(some_rows) == 0 or some_rows is None:
                print("\nNo more rows.")
                break
            for row in some_rows:
                count += 1
                iter = [str(item) for item in row]
                print( "Row %d:\t" % (count) + "\t".join(iter) )
            # Maybe print another batch.
            if len(some_rows) < array_size:
                print("\nNo more rows.")
                break
            prompt = "\nHit Enter to see rows %d-%d, (Q) to Quit, or (N) for No more rows: " % (count+1,count+array_size)
            more_rows = ask_end_user( prompt ).upper() # N/None
            if more_rows.upper() == "N":
                break
    else: # All SQL besides select.
        # cursor.rowcount useless for sqlite3
        rows_affected = cursor.rowcount
        print("%d rows affected" % (rows_affected))
        print(sql_type)

    return

def begin_trans( connection ):
    # Does this db type do begin trans?  @@@@@@@@@@@@@@@@@@@@@@@@@@@@
    return

def commit( connection, SQL ):
    sql_type = SQL.split()[0].upper()
    # Does this db type do commits?  @@@@@@@@@@@@@@@@@@@@@@@@@@@@
    if sql_type != "SELECT":
        commit = ask_end_user("\nDo you want to commit? (y/n): ").upper()
        if commit in ("Y", "T", "1"):
            print("Commit done.")
            connection.commit()
    return

def explain_plan( cursor, SQL ): # @@@@@@@@@@@@@@@@@@@@@
    return

def disconnect_db( db_type, connection, cursor ):
    # Disconnect from db.
    condition = db_type in db_has_close
    if condition and cursor is not None:
        cursor.close()
    if condition and connection is not None:
        connection.close()
    return

def do_stacktrace():
    # Print stack trace but resume program.
    print()
    exc_type, exc_value, exc_traceback = sys.exc_info()
    traceback.print_exception(exc_type, exc_value, exc_traceback, limit=None, file=sys.stdout)
    return

def ask_end_user( prompt ):
    # The only place for end-user input, except in ask_for_users_login (getpass).
    response = input(prompt).strip()
    uresponse = response.upper()
    if   uresponse == "Q":
        raise Exception_Quit()
    elif uresponse == "S":
        raise Exception_StartOver()
    elif uresponse == "N":
        raise Exception_NewDB()
    elif uresponse == "V":
        pass

    return response

########## MAIN

# Main loop.  Keep looping until break.
while True:

    db_type    = None
    connection = None
    cursor     = None
    try:
        # Ask end-user for db type.
        db_type = ask_for_db_type() # Q/Exception_Quit

        # Ask end-user for db location (either file path or hostname/port).
        db_location = ask_for_db_location( db_type ) # Q/Exception_Quit, S/Exception_StartOver

        # Ask end-user for db instance if this db type has them.
        db_instance = ask_for_db_instance( db_type ) # Q/Exception_Quit, S/Exception_StartOver

        # Ask end-user for login (username, password) if this db type has them.
        db_login = ask_for_db_login( db_type ) # Q/Exception_Quit, S/Exception_StartOver

        # Connect to db (see if connection fails before asking for SQL).
        connection = connect_to_db(db_type, db_location, db_instance, db_login) # Generic Exception

        while True:
            # Begin Transaction??  @@@@@@@@@@@@@@@@@@@@@@@@@@@@
            # Ask end-user for SQL to run.
            # Or examine data dictionary.
            # Bind variables? @@@@@@@@@@@@@@@@@@@@@@@@@@@@
            SQL = ask_for_sql() # Q/Exception_Quit, N/Exception_NewDB
            # Execute SQL
            try:
                cursor = run_sql( connection, SQL ) # Exception_BadSQL
                # Explain plan?
                #explain_plan( cursor, SQL ) # @@@@@@@@@@@@@@@@@@@@@@@@@@@@
                # Print query results or db response.
                print_response( cursor, SQL )
            except Exception_BadSQL as e:
                # Just go to next SQL statement.
                pass
            # Commit SQL, if an option. @@@@@@@@@@@@@@@@@@@@@@@@@@@@
            #commit( connection, SQL )
    except Exception_Fatal as e:
        # Not yet implemented.
        # Sender already announced what the problem is.
        break
    except Exception_Quit as e:
        print("\nQuitting at your request.")
        break
    except Exception_StartOver as e:
        print("\nStarting over at your request.")
    except Exception_NewDB as e:
        pass
    except Exception as e:
        do_stacktrace()
        print("\nWill continue, if possible.")
    finally:
        disconnect_db( db_type, connection, cursor )

########## IF __NAME__ ....

if __name__ == "__main__":
    pass
