""" UniversalClient_Complex.py

SUMMARY: Command-line universal database client, the version with automated
         generation of SQL and Bind Variables.

REPOSITORY: https://github.com/DavidJLambert/Python-Universal-DB-Client

AUTHOR: David J. Lambert

VERSION: 0.7.4

DATE: Apr 19, 2020

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
sample_hostname = {
    access: '',
    mysql: '127.0.0.1',
    oracle: 'DESKTOP-C54UGSE.attlocal.net',
    postgresql: '127.0.0.1',
    sqlite: '',
    sqlserver: '127.0.0.1'}

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

bind_var_fmt = dict()
bind_var_fmt[access] = '{}'
bind_var_fmt[mysql] = '@{}'
bind_var_fmt[oracle] = ':{}'
bind_var_fmt[postgresql] = '${}'
bind_var_fmt[sqlite] = ':{}'
bind_var_fmt[sqlserver] = '@{}'

# BIND VARIABLE NAME FORMAT USING PYTHON DB API 2.0 LIBRARY

bind_var_fmt[named] = ':{}'
bind_var_fmt[pyformat] = '%({})s'
bind_var_fmt[qmark] = '?{}'

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
data_type = 'data_type'
data_type_group = 'data_type_group'

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
    empty = [''] * num_bind_vars
    bind_var_table = empty[:]
    bind_var_column = empty[:]
    bind_var_value = empty[:]
    bind_var_data_type = empty[:]
    bind_var_data_type_group = empty[:]

    bind_var_table[0] = 'PRODUCTS'
    bind_var_column[0] = 'actor'
    bind_var_value[0] = 'CHEVY FOSTER'

    bind_var_table[1] = 'PRODUCTS'
    bind_var_column[1] = 'price'
    bind_var_value[1] = 35.00

    # Separate list for command line, because of string quoting.
    bind_var_value_cmd = bind_var_value[:]

    # Get bind variable data types.
    if mq.data_dict_sql[mq.tables, db_type1] == mq.not_possible_sql:
        # No data dictionary to use, will have to manually construct query.
        pass
    else:
        for key in range(num_bind_vars):
            (bind_var_data_type[key],
             bind_var_data_type_group[key]) = my_db_client.get_data_type(
                bind_var_table[key], bind_var_column[key])
            # Quote strings.
            if bind_var_data_type_group[key] == 'STRING':
                bind_var_value_cmd[key] = quote_a_string(bind_var_value[key])

    bind_var = empty[:]
    for key in range(num_bind_vars):
        if db_type1 == postgresql:
            bind_var[key] = '$' + str(key + 1)
        else:
            bind_var[key] = bind_var_fmt[db_type1].format(bind_var_column[key])

    # ADAPT QUERY TO GIVEN DATABASE TYPE
    query1 = query.format(bind_var[0], bind_var[1],
                          terminator=terminator[db_type1])
    # CONSTRUCT COMMANDS AROUND QUERY TO RUN IN DATABASE COMMAND-LINE CLIENT.
    pre_cmd = ''
    set_bind_vars = ''
    post_cmd = ''
    db_client_exe = db_client_exes[db_type1].upper()
    if db_client_exe == '':
        z = '{} DOES NOT HAVE A COMMAND LINE INTERFACE.'
        print(z.format(db_type1).upper())
    elif db_type1 == mysql:
        # SET @x := 'y';                 Create variable "x", give it value "y"
        set_value = "SET {} := {};\n"
        for key in range(num_bind_vars):
            set_bind_vars += set_value.format(
                bind_var_fmt[mysql].format(bind_var_column[key]),
                bind_var_value_cmd[key])
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
        set_value = "VARIABLE {name} {type}\n"
        for key in range(num_bind_vars):
            ora_type = bind_var_data_type[key]
            if bind_var_data_type[key][:7] == 'NUMBER(':
                ora_type = 'NUMBER'
            set_bind_vars += set_value.format(
                name=bind_var_column[key],
                type=ora_type)
        set_bind_vars += "BEGIN\n"
        set_value = "    {name} := {value};\n"
        for key in range(num_bind_vars):
            ora_type = bind_var_data_type[key]
            if bind_var_data_type[key][:7] == 'NUMBER(':
                ora_type = 'NUMBER'
            set_bind_vars += set_value.format(
                name=bind_var_fmt[oracle].format(bind_var_column[key]),
                value=bind_var_value_cmd[key])
        set_bind_vars += (
            "END;\n"
            "/\n")
        post_cmd = 'exit\n'
    elif db_type1 == postgresql:
        # \pset footer off             Turn off query output footer (# rows)
        # \pset fieldsep |             Set column separator to | (pipe)
        pre_cmd = (
            "\\pset footer off\n"
            "\\pset fieldsep {}\n")
        set_value = "PREPARE x9q7z ({}) AS "
        types = [bind_var_data_type[key] for key in range(num_bind_vars)]
        types = ','.join(types)
        set_bind_vars = set_value.format(types)
        set_value = "EXECUTE x9q7z ({});\n"
        values = [str(bind_var_value_cmd[key]) for key in range(num_bind_vars)]
        values = ','.join(values)
        post_cmd = set_value.format(values) + "\\quit\n"
    elif db_type1 == sqlite:
        # .echo off                      Set command echo off
        # .separator "|"                 Set column separator to pipe character
        # .headers on                    Put in column headings (column names)
        # .parameter set :x 3            Create variable "x", set it to 3
        pre_cmd = (
            '.echo off\n'
            '.separator "{}"\n'
            '.headers on\n')
        set_value = ".parameter set {} {}\n"
        for key in range(num_bind_vars):
            my_value = bind_var_value_cmd[key]
            set_bind_vars += set_value.format(
                bind_var_fmt[db_type1].format(bind_var_column[key]),
                my_value)
        post_cmd = '.exit\n'
    elif db_type1 == sqlserver:
        # :Setvar SQLCMDCOLSEP |         Set column separator to pipe character
        # SET NOCOUNT ON                 Turn off "rows affected"
        # DECLARE @x AS VARCHAR(9);      Create a varchar(9) variable named "x"
        # SET @x = 'y';                  Set value of "x" to "y"
        pre_cmd = (
            ":Setvar SQLCMDCOLSEP {}\n"
            "SET NOCOUNT ON\n")
        set_value = (
            "DECLARE {name} AS {type};\n"
            "SET {name} = {value};\n")
        for key in range(num_bind_vars):
            set_bind_vars += set_value.format(
                name=bind_var_fmt[db_type1].format(bind_var_column[key]),
                value=bind_var_value_cmd[key],
                type=bind_var_data_type[key])
        post_cmd = (
            'go\n'
            'exit\n')

    # ASSEMBLE COMMAND PARTS INTO COMPLETE COMMAND.
    if pre_cmd.find('{}') > -1:
        pre_cmd = pre_cmd.format(my_colsep)
    client_cmds = pre_cmd + set_bind_vars + query1 + post_cmd

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
    bind_vars2 = db_instance1.init_bind_vars()
    paramstyle2 = db_instance1.get_paramstyle()

    # SQL & BIND VARIABLES TO EXECUTE THROUGH DB API 2.0 LIBRARY.
    query2 = ''
    if db_type1 == access:
        # MS Access does not support bind variables/parameterization.
        # No data dictionary, so construct query manually.
        query2 = query.format(quote_a_string(bind_var_value[0]),
                              bind_var_value[1], terminator='')
        bind_vars2 = ''
    elif paramstyle2 == named:
        for key in range(num_bind_vars):
            # Bind variables kept in dictionary.
            bind_vars2[bind_var_column[key]] = bind_var_value[key]
        query2 = query.format(
            bind_var_fmt[paramstyle2].format(bind_var_column[0]),
            bind_var_fmt[paramstyle2].format(bind_var_column[1]),
            terminator='')
    elif paramstyle2 == qmark:
        for key in range(num_bind_vars):
            # Bind variables kept in tuple.
            bind_vars2 += (bind_var_value[key],)
        query2 = query.format('?', '?', terminator='')
    elif paramstyle2 == pyformat:
        for key in range(num_bind_vars):
            # Bind variables kept in dictionary.
            bind_vars2[bind_var_column[key]] = bind_var_value[key]
        query2 = query.format(
            bind_var_fmt[paramstyle2].format(bind_var_column[0]),
            bind_var_fmt[paramstyle2].format(bind_var_column[1]),
            terminator='')
    else:
        z = 'DB type {}, with library {}, has an unknown parameter style.'
        print(z.format(db_type1, lib_name_for_db[db_type1]))
        my_db_client.clean_up()
        exit(1)

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
