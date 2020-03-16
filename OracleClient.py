""" OracleClient.py

SUMMARY:
  Class DBClient executes SQL with bind variables.

REPOSITORY: https://github.com/DavidJLambert/Python-Universal-DB-Client

AUTHOR: David J. Lambert

VERSION: 0.5.0

DATE: Mar 16, 2020

PURPOSE:
  A sample of my Python coding, to demonstrate that I can write decent Python,
  test it, and document it.  I also demonstrate I know relational databases.

DESCRIPTION:
  This is a command-line program that asks an end-user for SQL to execute on
  1 of 6 types of relational databases, ordered by popularity as ranked in
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

  Class DBInstance encapsulates all the info needed to log into a database
  instance, plus it encapsulates the connection handle to that database.
  Its externally useful methods are:
  1)  print_all_connection_parameters: prints all the connection parameters.
  2)  close_connection: closes the connect to the database.
  3)  get_connection_status: whether or not DBInstance is connected to the db.

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

# The import hierarchy:
# DBClient <- OutputWriter <- MyFunctions <- MyConstants & sys
#     ^------ DBInstance   <- MyConstants

from DBClient import *

# -------- CREATE AND INITIALIZE VARIABLES
# See MyConstants.py.

# -------- CUSTOM CLASSES
# See DBInstance.py, OutputWriter.py, and DBClient.py.

# -------- CUSTOM STAND-ALONE FUNCTIONS
# See MyFunctions.py.

# LET'S ACTUALLY RUN SOMETHING.
if __name__ == '__main__':

    # OS AND PYTHON VERSION STUFF
    os_python_version_info()

    # COLUMN SEPARATOR FOR OUTPUT.
    my_colsep = '|'

    # GET DATABASE INSTANCE TO USE.
    db_type1 = mysql
    db_path1 = ''
    username1 = ''
    password1 = ''
    hostname1 = ''
    port_num1 = 0
    instance1 = ''
    sql_for_client = ''
    if db_type1 == oracle:
        username1 = 'ds2'
        password1 = 'ds2'
        hostname1 = 'DESKTOP-C54UGSE.attlocal.net'
        port_num1 = 1521
        instance1 = 'XE'
    elif db_type1 == sql_server:
        username1 = 'sa'
        password1 = 'password'
        hostname1 = '192.168.1.64'
        port_num1 = 1433
        instance1 = 'DS2'
    elif db_type1 == mysql:
        username1 = 'root'
        password1 = 'password'
        hostname1 = '192.168.1.71'
        port_num1 = 3306
        instance1 = 'DS2'
    elif db_type1 == postgresql:
        username1 = 'root'
        password1 = 'password'
        hostname1 = '192.168.1.67'
        port_num1 = 5432
        instance1 = 'ds2'
    elif db_type1 == access:
        db_path1 = r'.\databases\ds2.accdb'
    elif db_type1 == sqlite:
        db_path1 = './databases/ds2.sqlite3'
    else:
        print('Unknown database type.')
        exit(1)

    if db_type1 == oracle:
        # TEXT OF COMMANDS TO RUN IN SQLPLUS FOR ORACLE.
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
        sql_for_client = """
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
        FROM PRODUCTS p INNER JOIN CATEGORIES c
        ON p.category = c.category
        WHERE actor = :actor;
        exit
        """.format(username1, password1, hostname1, port_num1, instance1,
                   my_colsep)
    elif db_type1 == sqlite:
        # UNFINISHED
        # TEXT OF COMMANDS TO RUN IN SQLITE.
        # Explanation of commands:
        # .open {}      turn off prompt
        sql_for_client = """
        .open "{}"
        .separator "{}"
        .headers on
        .nullvalue "(null)"
        VARIABLE actor VARCHAR2(50)
        VARIABLE whatever NUMBER
        BEGIN
            :actor := 'CHEVY FOSTER';
        END;
        /
        SELECT actor, title, price, categoryname
        FROM PRODUCTS p INNER JOIN CATEGORIES c
        ON p.category = c.category
        WHERE actor = :actor;
        exit
        """.format(db_path1, my_colsep)
    elif db_type1 == sql_server:
        # TEXT OF COMMANDS TO RUN IN SQLCMD FOR SQL SERVER.
        # Explanation of commands:
        sql_for_client = """
        DECLARE @actor AS VARCHAR(50);
        SET @actor = 'CHEVY FOSTER';
        DECLARE @whatever AS INT;
        SELECT actor, title, price, categoryname
        FROM PRODUCTS p INNER JOIN CATEGORIES c
        ON p.category = c.category
        WHERE actor = @actor
        exit
        """.format(db_path1, my_colsep)
    elif db_type1 == access:
        print("MS Access does not have a command line interface.")

    # TODO disable cmdline client for now.
    '''
    # RUN ABOVE COMMANDS IN DATABASE CLIENT.
    print('\nRUNNING COMMANDS IN DATABASE CLIENT...')
    # client_output = run_sql_cmdline(sql_for_client, db_type1)

    # SHOW THE OUTPUT FROM RUNNING ABOVE COMMANDS IN SQLPLUS.
    # Don't use write_rows, it'll crash because sqlplus output not all columnar.
    print('\nTHE OUTPUT FROM RUNNING THOSE COMMANDS IN SQLPLUS:')
    if client_output != '':
        for line in client_output:
            print(line)
    '''
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
    # my_db_client.database_table_schema(colsep=my_colsep)
    print()

    # See the database view schema for my login.
    print('\nSEE THE DEFINITION AND COLUMNS OF ONE VIEW...')
    # my_db_client.database_view_schema(colsep=my_colsep)
    print()

    # Pass in text of SQL and a dict of bind variables and their values.
    sql1 = ''
    bind_vars1 = dict()
    if db_type1 == access:
        # MS Access does not support bind variables/parameterization.
        sql1 = ("SELECT actor, title, price, categoryname "
                "FROM PRODUCTS p INNER JOIN CATEGORIES c "
                "ON p.category = c.category WHERE actor = 'CHEVY FOSTER'")
    elif lib_name_for_db[db_type1] in paramstyle_named:
        sql1 = ("SELECT actor, title, price, categoryname "
                "FROM PRODUCTS p INNER JOIN CATEGORIES c "
                "ON p.category = c.category WHERE actor = :actor")
        bind_vars1 = {'actor': 'CHEVY FOSTER'}
    elif lib_name_for_db[db_type1] in paramstyle_qmark:
        sql1 = ("SELECT actor, title, price, categoryname "
                "FROM PRODUCTS p INNER JOIN CATEGORIES c "
                "ON p.category = c.category WHERE actor = ?")
        bind_vars1 = ('CHEVY FOSTER',)
    elif lib_name_for_db[db_type1] in paramstyle_pyformat:
        sql1 = ("SELECT actor, title, price, categoryname "
                "FROM PRODUCTS p INNER JOIN CATEGORIES c "
                "ON p.category = c.category WHERE actor = %(actor)s")
        bind_vars1 = {'actor': 'CHEVY FOSTER'}
    else:
        z = 'DB type {}, with library {}, has an unknown parameter style.'
        print(z.format(db_type1, lib_name_for_db[db_type1]))

    print("\nHERE'S SOME SQL:\n{}".format(sql1))
    if len(bind_vars1) > 0:
        print('\nHERE ARE THE BIND VARIABLES:\n{}'.format(bind_vars1))
    my_db_client.set_sql(sql1, bind_vars1)

    # Execute the SQL.
    print('\nGETTING THE OUTPUT OF THAT SQL:')
    col_names1, rows1, row_count1 = my_db_client.run_sql()

    # Set up to write output.
    print('\nPREPARING TO FORMAT THAT OUTPUT, AND PRINT OR WRITE IT TO FILE.')
    writer = OutputWriter(out_file_name='', align_col=True, col_sep='|')
    writer.get_align_col()
    writer.get_col_sep()
    writer.get_out_file_name()
    # Show the results.
    print("\nHERE'S THE OUTPUT...")
    writer.write_rows(rows1, col_names1)

    # Clean up.
    print('\n\nOK, FORGET ALL THAT.')
    writer.close_output_file()
    col_names1 = None
    rows1 = None

    # From a prompt, read in SQL & dict of bind variables & their values.
    my_db_client.set_sql_at_prompt()
    if db_type1 != access:
        my_db_client.set_bind_vars_at_prompt()

    # Execute the SQL.
    print('\nGETTING THE OUTPUT OF THAT SQL:')
    col_names2, rows2, row_count2 = my_db_client.run_sql()

    # Set up to write output.
    print('\nPREPARING TO FORMAT THAT OUTPUT, AND PRINT OR WRITE IT TO FILE.')
    writer = OutputWriter(out_file_name='', align_col=True, col_sep='|')
    writer.get_align_col()
    writer.get_col_sep()
    writer.get_out_file_name()
    # Show the results.
    writer.write_rows(rows2, col_names2)

    # Clean up.
    writer.close_output_file()
    del writer
    my_db_client.clean_up()
    del my_db_client
    col_names2 = None
    rows2 = None
    print()
    db_instance1.close_connection(del_cursors=True)
    print(db_instance1.get_connection_status())
    del instance1
