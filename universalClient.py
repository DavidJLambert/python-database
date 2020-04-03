""" universalCLient.py

SUMMARY: Command-line universal database client.

REPOSITORY: https://github.com/DavidJLambert/Python-Universal-DB-Client

AUTHOR: David J. Lambert

VERSION: 0.7.0

DATE: Apr 2, 2020

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

sample_hostname = {
    access: '',
    mysql: '192.168.1.71',
    oracle: 'DESKTOP-C54UGSE.attlocal.net',
    postgresql: '192.168.1.67',
    sqlite: '',
    sqlserver: '192.168.1.64'}

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

# BIND VARIABLE NAME FORMAT AT COMMAND LINE

bind_var_fmt_cmd = {
    access: '{}',
    mysql: '@{}',
    oracle: ':{}',
    postgresql: '${}',
    sqlite: ':{}',
    sqlserver: '@{}'}

# BIND VARIABLE NAME FORMAT USING PYTHON DB API 2.0 LIBRARY

bind_var_fmt_lib = {
    access: '{}',
    named: ':{}',
    pyformat: '%({})s',
    qmark: '?{}'}

# OPTION FOR SETTING COLUMN SEPARATOR

col_sep_option = {
    access: False,
    mysql: False,
    oracle: True,
    postgresql: True,
    sqlite: True,
    sqlserver: True}

# QUERY TERMINATION CHARACTERS AT COMMAND LINE

terminator = {
    access: '',
    mysql: ';',
    oracle: ';',
    postgresql: ';',
    sqlite: ';',
    sqlserver: '\ngo'}

# Fields for bind_vars
table = 'table'
column = 'column'
value = 'value'
data_type_native = 'data_type_native'
data_type_dbapi2 = 'data_type_dbapi2'

# -------- MAIN PROGRAM

if __name__ == '__main__':

    # OS AND PYTHON VERSION STUFF
    os, py_version_major, py_version_minor = os_python_version_info()

    # GET DATABASE CONNECTION INFO TO USE.
    db_type1 = access

    if db_type1 not in db_types:
        print('UNKNOWN DATABASE TYPE.')
        exit(1)
    db_path1 = sample_db_path[db_type1]
    username1 = sample_username[db_type1]
    password1 = sample_password
    hostname1 = sample_hostname[db_type1]
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

    # Count number of bind variables.
    num_bind_vars = query.count('{')
    if num_bind_vars != query.count('}'):
        print('The number of opening and closing braces are unequal!')
        exit(1)
    # Exclude {terminator} from count of parameters.
    num_bind_vars -= 1

    # Info for constructing bind variables.
    bind_var_data = dict()
    bind_var_data[0] = {
        table: 'PRODUCTS', column: 'actor', value: 'CHEVY FOSTER',
        data_type_native: 'TBD', data_type_dbapi2: 'TBD'}
    bind_var_data[1] = {
        table: 'PRODUCTS', column: 'price', value: 35.00,
        data_type_native: 'TBD', data_type_dbapi2: 'TBD'}

    # Get bind variable data types.
    for key in range(num_bind_vars):
        x, y = my_db_client.get_data_type(bind_var_data[key][table],
                                          bind_var_data[key][column])
        bind_var_data[key][data_type_native] = x
        bind_var_data[key][data_type_dbapi2] = y

    bind_var = [''] * num_bind_vars
    for key in range(num_bind_vars):
        bind_var[key] = bind_var_fmt_cmd[db_type1].format(
            bind_var_data[key][column])
    # ADAPT QUERY TO GIVEN DATABASE TYPE
    query1 = query.format(bind_var[0], bind_var[1],
                          terminator=terminator[db_type1])
    # CONSTRUCT COMMANDS AROUND QUERY TO RUN IN DATABASE COMMAND-LINE CLIENT.
    client_cmds = ''
    pre_cmd = ''
    set_bind_vars = ''
    post_cmd = ''
    if db_type1 == access:
        pass
    elif db_type1 == mysql:
        # \option resultFormat=table     Use the "table" result format
        # SET @x := 'y';                 Create variable "x", give it value "y"
        pre_cmd = "\\option resultFormat=table"
        template = "SET {} := {};\n"
        set_bind_vars = ''
        for key in range(num_bind_vars):
            set_bind_vars += template.format(bind_var_data[key][column],
                                             bind_var_data[key][value])
        post_cmd = '\\exit\n'
    elif db_type1 == oracle:
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
        pre_cmd = (
            'SET SQLPROMPT ""\n'
            'SET SQLNUMBER OFF\n'
            'SET TRIMOUT ON\n'
            'SET TAB OFF\n'
            'SET NEWPAGE NONE\n'
            'SET LINESIZE 256\n'
            'SET WRAP OFF\n'
            'SET COLSEP "{}"\n')
        template = (
            "VARIABLE {name} {type}\n"
            "BEGIN\n"
            "    {name} := {value};\n"
            "END;\n"
            "/\n")
        set_bind_vars = ''
        for key in range(num_bind_vars):
            set_bind_vars += template.format(
                name=bind_var_data[key][column],
                value=bind_var_data[key][value],
                type=bind_var_data[key][data_type_native])
        post_cmd = 'exit\n'
    elif db_type1 == postgresql:
        # \pset footer off             Turn off query output footer (# rows)
        # \pset fieldsep |             Set column separator to | (pipe)
        pre_cmd = (
            "\\pset footer off\n"
            "\\pset fieldsep {}\n")
        template = "PREPARE x9q7z ({}) AS "
        types = [bind_var_data[key][data_type_native] for
                 key in range(num_bind_vars)]
        types = ','.join(types)
        set_bind_vars = template.format(types)
        template = "EXECUTE x9q7z ({});\n"
        values = [bind_var_data[key][value] for key in range(num_bind_vars)]
        values = ','.join(values)
        post_cmd = template.format(values) + "\nexit\n"
    elif db_type1 == sqlite:
        # .echo off                      Set command echo off
        # .separator "|"                 Set column separator to pipe character
        # .headers on                    Put in column headings (column names)
        # .parameter set :x 3            Create variable "x", set it to 3
        pre_cmd = ('.echo off\n'
                   '.separator "{}"\n'
                   '.headers on\n')
        template = ".parameter set {} {}\n"
        set_bind_vars = ''
        for key in range(num_bind_vars):
            my_value = bind_var_data[key][value]
            if isinstance(my_value, str):
                my_value = "'" + my_value + "'"
            set_bind_vars += template.format(
                bind_var_fmt_cmd[db_type1].format(bind_var_data[key][column]),
                my_value)
        post_cmd = '.exit\n'
    elif db_type1 == sqlserver:
        # :Setvar SQLCMDCOLSEP |         Set column separator to pipe character
        # SET NOCOUNT ON                 Turn off "rows affected"
        # DECLARE @x AS VARCHAR(9);      Create a varchar(9) variable named "x"
        # SET @x = 'y';                  Set value of "x" to "y"
        pre_cmd = (":Setvar SQLCMDCOLSEP {}\n"
                   "SET NOCOUNT ON\n")
        template = (
            "DECLARE {name} AS {type};\n"
            "SET {name} = {value};\n")
        set_bind_vars = ''
        for key in range(num_bind_vars):
            set_bind_vars += template.format(
                name=bind_var_data[key][column],
                value=bind_var_data[key][value],
                type=bind_var_data[key][data_type_native])
        post_cmd = ('go\n'
                    'exit\n')

    # ASSEMBLE COMMAND PARTS INTO COMPLETE COMMAND.
    if col_sep_option:
        client_cmds = pre_cmd.format(my_colsep)
    else:
        client_cmds = pre_cmd
    client_cmds += set_bind_vars + query1 + post_cmd

    # RUN ABOVE COMMANDS IN DATABASE COMMAND-LINE CLIENT.
    db_client_exe = db_client_exes[db_type1].upper()
    z = '\nRUNNING COMMANDS IN {} COMMAND-LINE CLIENT...'
    print(z.format(db_client_exe))
    cmdline_list = db_instance1.get_cmdline_list()
    sql_out = sql_cmdline(cmdline_list, client_cmds)

    # SHOW OUTPUT FROM RUNNING ABOVE COMMANDS IN DATABASE COMMAND-LINE CLIENT.
    # Don't use write_rows, output often not all columnar, will cause crash.
    if len(sql_out) > 0:
        z = '\nTHE OUTPUT FROM RUNNING COMMANDS IN {} COMMAND-LINE CLIENT:'
        print(z.format(db_client_exe))
        for line in sql_out:
            print(line)
    print('ALL DONE WITH {} COMMAND-LINE CLIENT.'.format(db_client_exe))

    # DONE WITH COMMANDS IN DATABASE COMMAND-LINE CLIENT.
    # START CONSTRUCTING QUERY AND BIND VARIABLES TO RUN IN DB API 2.0 LIBRARY.
    z = "\nLET'S RUN THE SAME SQL THROUGH A DB API 2.0 LIBRARY"
    print(z.format(db_client_exe))
    bind_vars2 = db_instance1.init_bind_vars()
    paramstyle2 = db_instance1.get_paramstyle()

    # SQL & BIND VARIABLES TO EXECUTE THROUGH DB API 2.0 LIBRARY.
    query2 = ''
    if db_type1 == access:
        # MS Access does not support bind variables/parameterization.
        for key in range(num_bind_vars):
            if bind_var_data[key][data_type_dbapi2] in {'str', 'TEXT'}:
                bind_var_data[key][value] = quote_a_string(
                    bind_var_data[key][value])
        query2 = query.format(bind_var_data[0][value],
                              bind_var_data[1][value], terminator='')
        bind_vars2 = ''
    elif paramstyle2 == named:
        for key in range(num_bind_vars):
            # Bind variables kept in dictionary.
            bind_vars2[bind_var_data[key][column]] = bind_var_data[key][value]
        query2 = query.format(
            bind_var_fmt_lib[paramstyle2].format(bind_var_data[0][column]),
            bind_var_fmt_lib[paramstyle2].format(bind_var_data[1][column]),
            terminator='')
    elif paramstyle2 == qmark:
        for key in range(num_bind_vars):
            # Bind variables kept in tuple.
            bind_vars2 += (bind_var_data[key][value],)
        query2 = query.format('?', '?', terminator='')
    elif paramstyle2 == pyformat:
        for key in range(num_bind_vars):
            # Bind variables kept in dictionary.
            bind_vars2[bind_var_data[key][column]] = bind_var_data[key][value]
        query2 = query.format(
            bind_var_fmt_lib[paramstyle2].format(bind_var_data[0][column]),
            bind_var_fmt_lib[paramstyle2].format(bind_var_data[1][column]),
            terminator='')
    else:
        z = 'DB type {}, with library {}, has an unknown parameter style.'
        print(z.format(db_type1, lib_name_for_db[db_type1]))
        my_db_client.clean_up()
        exit(1)

    # SHOW THE SQL & BIND VARIABLES TO EXECUTE THROUGH DB API 2.0 LIBRARY.
    print("\nHERE'S SOME SQL:\n{}".format(query2))
    my_db_client.set_sql(query2)
    if len(bind_vars2) > 0:
        print('\nHERE ARE THE BIND VARIABLES:\n{}'.format(bind_vars2))
        my_db_client.set_bind_vars(bind_vars2)

    # EXECUTE THE SQL & BIND VARIABLES THROUGH DB API 2.0 LIBRARY.
    print('\nGETTING THE OUTPUT OF THAT SQL:')
    col_names1, col_types1, rows1, row_count1 = my_db_client.run_sql()

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
