""" MyConstants.py """

# Supported database types.
oracle = 'oracle'
mysql = 'mysql'
sql_server = 'sql server'
postgresql = 'postgresql'
access = 'access'
sqlite = 'sqlite'
db_types = [oracle, mysql, sql_server, postgresql, access, sqlite]

# Groupings of database types.
uses_connection_string = set(db_types) - {mysql}
file_databases = {access, sqlite}

# Database libraries.
cx_Oracle = 'cx_Oracle'
pymysql = 'pymysql'
pyodbc = 'pyodbc'
psycopg2 = 'psycopg2'
sqlite3 = 'sqlite3'
lib_name_for_db = {oracle: cx_Oracle, mysql: pymysql, sql_server: pyodbc,
                   postgresql: psycopg2, access: pyodbc, sqlite: sqlite3}

# Parameterization/bind variable format used here.
paramstyle_named = {cx_Oracle, sqlite3}
paramstyle_pyformat = {pymysql, psycopg2}
paramstyle_qmark = {pyodbc}
