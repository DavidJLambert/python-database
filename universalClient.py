""" universalCLient.py

SUMMARY: Command-line universal database client.

REPOSITORY: https://github.com/DavidJLambert/Python-Universal-DB-Client

AUTHOR: David J. Lambert

VERSION: 0.6.4

DATE: Mar 26, 2020

For more information, see README.rst.
"""

# -------- IMPORTS

# The import hierarchy:
#     v------- MyQueries    <-- MyConstants
# DBClient <-- OutputWriter <-- MyFunctions <-- MyConstants & sys
#     ^------- DBInstance   <-- MyConstants

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
    os, py_version_major, py_version_minor = os_python_version_info()

    # COLUMN SEPARATOR FOR OUTPUT.
    my_colsep = '|'

    # GET DATABASE INSTANCE TO USE.
    db_type1 = oracle
    db_path1 = ''
    username1 = ''
    password1 = 'password'
    hostname1 = ''
    port_num1 = 0
    instance1 = ''
    sql_for_client = ''
    if db_type1 == access:
        db_path1 = (r'C:\Coding\PyCharm\projects\Python-Universal-DB-Client'
                    r'\databases\ds2.accdb')
    elif db_type1 == mysql:
        username1 = 'root'
        hostname1 = '192.168.1.71'
        port_num1 = 3306
        instance1 = 'DS2'
    elif db_type1 == oracle:
        username1 = 'ds2'
        hostname1 = 'DESKTOP-C54UGSE.attlocal.net'
        port_num1 = 1521
        instance1 = 'XE'
    elif db_type1 == postgresql:
        username1 = 'root'
        hostname1 = '192.168.1.67'
        port_num1 = 5432
        instance1 = 'ds2'
    elif db_type1 == sqlite:
        db_path1 = ('C:/Coding/PyCharm/projects/Python-Universal-DB-Client'
                    '/databases/ds2.sqlite3')
    elif db_type1 == sqlserver:
        username1 = 'sa'
        hostname1 = '192.168.1.64'
        port_num1 = 1433
        instance1 = 'DS2'
    else:
        print('UNKNOWN DATABASE TYPE.')
        exit(1)

    query = ('SELECT actor, title, price, categoryname\n'
             'FROM PRODUCTS p INNER JOIN CATEGORIES c\n'
             'ON p.category = c.category\n'
             'WHERE actor = ')
    if db_type1 == access:
        sql_for_client = ''
    elif db_type1 == mysql:
        # TEXT OF COMMANDS TO RUN IN MYSQLSH.
        # Explanation of commands:
        # SET @x := 'y';          Create variable "x", give it the value "y"
        z = "SET @actor := 'CHEVY FOSTER';\n"
        param = '@actor;'
        sql_for_client = z + query + param
    elif db_type1 == oracle:
        # TEXT OF COMMANDS TO RUN IN SQLPLUS.
        # Explanation of commands:
        # SET SQLPROMPT ""        Turn off prompt
        # SET SQLNUMBER OFF       Turn off numbers printed for multi-line input
        # SET TRIMOUT ON          Trim trailing spaces
        # SET TAB OFF             No tabs in the output
        # SET NEWPAGE NONE        Do nothing at page breaks
        # SET LINESIZE 256        Characters/line
        # SET WRAP OFF            Lines don't wrap, truncated to match LINESIZE
        # SET COLSEP "|"          Set column separator to pipe character
        # SPOOL <filename>        Writes output to <filename>, not standard out
        # SET TRIMSPOOL ON        Trim trailing spaces in output file (NOT USED)
        # VARIABLE x VARCHAR2(9)  Create a varchar(9) variable named "x"
        # BEGIN                   Set value of "x" to "y"
        #    :x := 'y';
        # END;
        # /
        z1 = ('SET SQLPROMPT ""\n'
              'SET SQLNUMBER OFF\n'
              'SET TRIMOUT ON\n'
              'SET TAB OFF\n'
              'SET NEWPAGE NONE\n'
              'SET LINESIZE 256\n'
              'SET WRAP OFF\n'
              'SET COLSEP "{}"\n'
              'VARIABLE actor VARCHAR2(50)\n'
              'BEGIN\n'
              "    :actor := 'CHEVY FOSTER';\n"
              'END;\n'
              '/\n')
        param = ':actor;'
        z2 = '\nexit\n'
        sql_for_client = z1.format(my_colsep) + query + param + z2
    elif db_type1 == postgresql:
        # TEXT OF COMMANDS TO RUN IN PSQL.
        # Explanation of commands:
        # \pset footer off             Turn off query output footer (# rows)
        z1 = ('\\pset footer off\n'
              'PREPARE x9q7z (text) AS ')
        param = "$1;"
        z2 = "\nEXECUTE x9q7z ('CHEVY FOSTER');\n"
        sql_for_client = z1 + query + param + z2
        print(sql_for_client)
    elif db_type1 == sqlite:
        # TEXT OF COMMANDS TO RUN IN SQLITE.
        # Explanation of commands:
        # .echo off                      Set command echo off
        # .separator "|"                 Set column separator to pipe character
        # .headers on                    Put in column headings (column names)
        # .parameter set :x 3            Create variable "x", set it to 3
        z1 = ('.echo off\n'
              '.separator "{}"\n'
              '.headers on\n'
              ".parameter set :actor 'CHEVY FOSTER'\n")
        param = ":actor;"
        z2 = '\n.exit\n'
        sql_for_client = z1.format(my_colsep) + query + param + z2
    elif db_type1 == sqlserver:
        # TEXT OF COMMANDS TO RUN IN SQLCMD.
        # Explanation of commands:
        # :Setvar SQLCMDCOLSEP |         Set column separator to pipe character
        # SET NOCOUNT ON                 Turn off "rows affected"
        # DECLARE @x AS VARCHAR(9);      Create a varchar(9) variable named "x"
        # SET @x = 'y';                  Set value of "x" to "y"
        z1 = (':Setvar SQLCMDCOLSEP {}\n'
              'SET NOCOUNT ON\n'
              'DECLARE @actor AS VARCHAR(50);\n'
              "SET @actor = 'CHEVY FOSTER';\n")
        param = '@actor'
        z2 = '\ngo\nexit\n'
        sql_for_client = z1.format(my_colsep) + query + param + z2

    # RUN ABOVE COMMANDS IN DATABASE CLIENT.
    db_client_exe = db_client_exes[db_type1].upper()
    print('\nRUNNING COMMANDS IN {}...'.format(db_client_exe))
    sql_out = sql_cmdline(os, sql_for_client, db_type1, db_path1, username1,
                          password1, hostname1, port_num1, instance1)

    # SHOW THE OUTPUT FROM RUNNING ABOVE COMMANDS IN DATABASE CLIENT.
    # Don't use write_rows, probably the db client output not all columnar,
    # it'll crash the database client.
    if len(sql_out) > 0:
        print('\nTHE OUTPUT FROM RUNNING COMMANDS IN {}:'.format(db_client_exe))
        for line in sql_out:
            print(line)
        print('ALL DONE WITH {}.'.format(db_client_exe))

    # CONNECT TO DATABASE INSTANCE SPECIFIED ABOVE.
    print('\nCONNECTING TO DATABASE...')
    db_instance1 = DBInstance(db_type1, db_path1, username1, password1,
                              hostname1, port_num1, instance1)
    db_instance1.print_all_connection_parameters()
    bind_vars1 = db_instance1.init_bind_vars()
    paramstyle1 = db_instance1.get_paramstyle()

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
    sql1 = ''
    if db_type1 == access:
        # MS Access does not support bind variables/parameterization.
        sql1 = query + "'CHEVY FOSTER'"
        bind_vars1 = ''
    elif paramstyle1 == named:
        sql1 = query + ':actor'
        bind_vars1['actor'] = 'CHEVY FOSTER'
    elif paramstyle1 == qmark:
        sql1 = query + '?'
        bind_vars1 += ('CHEVY FOSTER',)
    elif paramstyle1 == pyformat:
        sql1 = query + '%(actor)s'
        bind_vars1['actor'] = 'CHEVY FOSTER'
    else:
        z = 'DB type {}, with library {}, has an unknown parameter style.'
        print(z.format(db_type1, lib_name_for_db[db_type1]))
        exit(1)

    print("\nHERE'S SOME SQL:\n{}".format(sql1))
    my_db_client.set_sql(sql1)
    if len(bind_vars1) > 0:
        print('\nHERE ARE THE BIND VARIABLES:\n{}'.format(bind_vars1))
        my_db_client.set_bind_vars(bind_vars1)

    # Execute the SQL.
    print('\nGETTING THE OUTPUT OF THAT SQL:')
    col_names1, rows1, row_count1 = my_db_client.run_sql()

    # Set up to write output.
    print('\nPREPARING TO FORMAT THAT OUTPUT, AND PRINT OR WRITE IT TO FILE.')
    writer = OutputWriter(out_file_name='', align_col=True, col_sep=my_colsep)
    writer.get_align_col()
    writer.get_col_sep()
    writer.get_out_file_name()
    # Show the results.
    print("\nHERE'S THE OUTPUT...")
    writer.write_rows(rows1, col_names1)

    # Clean up.
    writer.close_output_file()
    del writer
    my_db_client.clean_up()
    del my_db_client
    col_names1 = None
    rows1 = None
    print()
    db_instance1.close_connection(del_cursors=True)
    print(db_instance1.get_connection_status())
    del instance1
