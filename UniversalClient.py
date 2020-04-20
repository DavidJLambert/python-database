""" UniversalClient.py

SUMMARY: Command-line universal database client, the version with manual
         specification of SQL and Bind Variables.

REPOSITORY: https://github.com/DavidJLambert/Python-Universal-DB-Client

AUTHOR: David J. Lambert

VERSION: 0.7.5

DATE: Apr 20, 2020

For more information, see README.rst.
"""
# -------- IMPORTS

# The import hierarchy:
#     v------- MyQueries    <-- MyConstants
# DBClient <-- OutputWriter <-- MyFunctions <-- sys
#     ^------- DBInstance   <-- MyFunctions <-- sys
#                   ^-----------MyConstants

from DBClient import *

# -------- CONSTANTS

# INFORMATION ABOUT SAMPLE DATABASES.
sample_username = {
    access: '',
    mysql: 'root',
    oracle: 'ds2',
    postgresql: 'root',
    sqlite: '',
    sqlserver: 'sa'}

sample_password = 'password'

# VirtualBox Guests use NAT.
sample_hostname = '127.0.0.1'

sample_port_num = {
    access: 0,
    mysql: 3306,
    oracle: 1521,
    postgresql: 5432,
    sqlite: 0,
    sqlserver: 1433}

sample_instance = {
    access: '',
    mysql: 'DS2',
    oracle: 'XE',
    postgresql: 'ds2',
    sqlite: '',
    sqlserver: 'DS2'}

home = 'C:\\Coding\\PyCharm\\projects\\Python-Universal-DB-Client\\'
sample_db_path = {
    access: r'databases\ds2.accdb',
    mysql: '',
    oracle: '',
    postgresql: '',
    sqlite: 'databases/ds2.sqlite3',
    sqlserver: ''}
sample_db_path[access] = home + sample_db_path[access]
home = home.replace('\\', '/')
sample_db_path[sqlite] = home + sample_db_path[sqlite]

# -------- MAIN PROGRAM

if __name__ == '__main__':

    # OS AND PYTHON VERSION STUFF
    os, py_version_major, py_version_minor = os_python_version_info()

    # GET DATABASE CONNECTION INFO TO USE.
    db_type1 = postgresql

    if db_type1 not in db_types:
        print('UNKNOWN DATABASE TYPE.')
        exit(1)
    db_path1 = sample_db_path[db_type1]
    username1 = sample_username[db_type1]
    password1 = sample_password
    hostname1 = sample_hostname
    port_num1 = sample_port_num[db_type1]
    instance1 = sample_instance[db_type1]

    # CONNECT TO DATABASE INSTANCE SPECIFIED ABOVE.
    print('\nCONNECTING TO DATABASE...')
    db_instance1 = DBInstance(os, db_type1, db_path1, username1, password1,
                              hostname1, port_num1, instance1)
    db_instance1.print_all_connection_parameters()

    # CREATE DATABASE CLIENT OBJECT.
    print('\nGETTING DATABASE CLIENT OBJECT...')
    my_db_client = DBClient(db_instance1)

    # COLUMN SEPARATOR FOR OUTPUT.
    my_colsep = '|'

    # SEE THE COLUMNS AND INDEXES OF ONE TABLE ACCESSIBLE TO GIVEN LOGIN.
    print('\nSEE THE COLUMNS AND INDEXES OF ONE TABLE...')
    my_db_client.db_table_schema(colsep=my_colsep)
    print()

    # SEE THE DEFINITION AND COLUMNS OF ONE VIEW ACCESSIBLE TO GIVEN LOGIN.
    print('\nSEE THE DEFINITION AND COLUMNS OF ONE VIEW...')
    my_db_client.db_view_schema(colsep=my_colsep)
    print()

    # SQL TO EXECUTE AT COMMAND LINE *AND* USING PYTHON DB API 2.0 LIBRARY
    query = ("SELECT actor, title, price, categoryname\n"
             "FROM PRODUCTS p INNER JOIN CATEGORIES c\n"
             "ON p.category = c.category\n"
             "WHERE actor = {0}\n"
             "AND price < {1}{terminator}\n")

    # CONSTRUCT COMMANDS AROUND QUERY TO RUN IN DATABASE COMMAND-LINE CLIENT.
    pre_cmd = ''
    query1 = ''
    post_cmd = ''
    db_client_exe = db_client_exes[db_type1].upper()
    if db_client_exe == '':
        z = '{} DOES NOT HAVE A COMMAND LINE INTERFACE.'
        print(z.format(db_type1).upper())
    elif db_type1 == mysql:
        pre_cmd = (
            "SET @actor := 'CHEVY FOSTER';\n"
            "SET @price := 35.0;\n")
        query1 = query.format('@actor', '@price', terminator=';')
    elif db_type1 == oracle:
        pre_cmd = (
            'SET SQLPROMPT ""\n'
            'SET SQLNUMBER OFF\n'
            'SET TRIMOUT ON\n'
            'SET TAB OFF\n'
            'SET NEWPAGE NONE\n'
            'SET LINESIZE 256\n'
            'SET WRAP OFF\n'
            'SET COLSEP "{}"\n'
            'VARIABLE actor VARCHAR2(50)\n'
            'VARIABLE price NUMBER\n'
            'BEGIN\n'
            "   :actor := 'CHEVY FOSTER';\n"
            "   :price := 35.0;\n"
            'END;\n'
            '/\n')
        query1 = query.format(':actor', ':price', terminator=';')
        post_cmd = 'exit\n'
    elif db_type1 == postgresql:
        pre_cmd = (
            "\\pset footer off\n"
            "\\pset fieldsep {}\n"
            "PREPARE x9q7z (text,numeric) AS\n")
        query1 = query.format('$1', '$2', terminator=';')
        post_cmd = (
            "EXECUTE x9q7z ('CHEVY FOSTER',35.0);\n"
            "\\quit\n")
    elif db_type1 == sqlite:
        # .echo off                      Set command echo off
        # .separator "|"                 Set column separator to pipe character
        # .headers on                    Put in column headings (column names)
        # .parameter set :x 3            Create variable "x", set it to 3
        pre_cmd = (
            '.echo off\n'
            '.separator "{}"\n'
            '.headers on\n'
            ".parameter set :actor 'CHEVY FOSTER'\n"
            ".parameter set :price 35.0\n")
        query1 = query.format(':actor', ':price', terminator=';')
        post_cmd = '.exit\n'
    elif db_type1 == sqlserver:
        pre_cmd = (
            ":Setvar SQLCMDCOLSEP {}\n"
            "SET NOCOUNT ON\n"
            "DECLARE @actor AS varchar(50);\n"
            "SET @actor = 'CHEVY FOSTER';\n"
            "DECLARE @price AS money;\n"
            "SET @price = 35.0;\n")
        query1 = query.format('@actor', '@price', terminator='')
        post_cmd = ('go\n'
                    'exit\n')

    # ASSEMBLE COMMAND PARTS INTO COMPLETE COMMAND.
    if pre_cmd.find('{}') > -1:
        pre_cmd = pre_cmd.format(my_colsep)
    client_cmds = pre_cmd + query1 + post_cmd

    # RUN ABOVE COMMANDS IN DATABASE COMMAND-LINE CLIENT.
    if db_client_exe != '':
        z = '\nRUNNING THESE COMMANDS IN {} COMMAND-LINE CLIENT:\n{}'
        print(z.format(db_client_exe, client_cmds))
        cmdline_list = db_instance1.get_cmdline_list()
        sql_out = sql_cmdline(cmdline_list, client_cmds)

        # SHOW OUTPUT FROM RUNNING ABOVE COMMANDS IN DB COMMAND-LINE CLIENT.
        # Don't use write_rows, output often not all columnar, will cause crash.
        if len(sql_out) > 0:
            z = '\nTHE OUTPUT FROM RUNNING COMMANDS IN {} COMMAND-LINE CLIENT:'
            print(z.format(db_client_exe))
            for line in sql_out:
                print(line)
            print('ALL DONE WITH {} COMMAND-LINE CLIENT.'.format(db_client_exe))

    # DONE WITH COMMANDS IN DATABASE COMMAND-LINE CLIENT.
    # START CONSTRUCTING QUERY AND BIND VARIABLES TO RUN IN DB API 2.0 LIBRARY.
    if db_client_exe != '':
        print("\nLET'S RUN THE SAME SQL THROUGH A DB API 2.0 LIBRARY")
    else:
        print("\nLET'S RUN SOME SQL THROUGH A DB API 2.0 LIBRARY")
    paramstyle2 = db_instance1.get_paramstyle()

    # SQL & BIND VARIABLES TO EXECUTE THROUGH DB API 2.0 LIBRARY.
    query2 = ''
    bind_vars2 = ''
    if paramstyle2 == nobindvars:
        # MS Access does not support bind variables/parameterization.
        query2 = query.format("'CHEVY FOSTER'", "35.0", terminator='')
    elif paramstyle2 == named:
        # oracle/cx_Oracle and sqlite/sqlite3.
        query2 = query.format(':actor', ':price', terminator='')
        bind_vars2 = {'actor': 'CHEVY FOSTER', 'price': 35.0}
    elif paramstyle2 == pyformat:
        # mysql/pymysql and postgresql/psycopg2.
        query2 = query.format("%(actor)s", "%(price)s", terminator='')
        bind_vars2 = {'actor': 'CHEVY FOSTER', 'price': 35.0}
    elif paramstyle2 == qmark:
        # sqlserver/pyodbc.
        query2 = query.format('?', '?', terminator='')
        bind_vars2 = ('CHEVY FOSTER', 35.0)

    # SHOW THE SQL & BIND VARIABLES TO EXECUTE THROUGH DB API 2.0 LIBRARY.
    print("\nHERE'S THE SQL:\n{}".format(query2))
    my_db_client.set_sql(query2)
    if len(bind_vars2) > 0:
        print('HERE ARE THE BIND VARIABLES:\n{}'.format(bind_vars2))
        my_db_client.set_bind_vars(bind_vars2)
    else:
        print('NO BIND VARIABLES.\n')
        my_db_client.set_bind_vars(bind_vars2)

    # EXECUTE THE SQL & BIND VARIABLES THROUGH DB API 2.0 LIBRARY.
    print('\nGETTING THE OUTPUT OF THAT SQL:')
    col_names1, rows1, row_count1 = my_db_client.run_sql()

    # SET UP TO WRITE OUTPUT OF SQL EXECUTED THROUGH DB API 2.0 LIBRARY.
    print('\nPREPARING TO FORMAT THAT OUTPUT, AND PRINT OR WRITE IT TO FILE.')
    writer = OutputWriter(out_file_name='', align_col=True, col_sep=my_colsep)
    writer.get_align_col()
    writer.get_col_sep()
    writer.get_out_file_name()

    # WRITE OUTPUT OF SQL & BIND VARS EXECUTED THROUGH DB API 2.0 LIBRARY.
    print("\nHERE'S THE OUTPUT...")
    writer.write_rows(rows1, col_names1)

    # CLEAN UP.
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
