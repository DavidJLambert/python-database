""" MyConstants.py """

# SUPPORTED DATABASE TYPES.

access = 'access'
mysql = 'mysql'
oracle = 'oracle'
postgresql = 'postgresql'
sqlite = 'sqlite'
sqlserver = 'sql server'
db_types = [access, mysql, oracle, postgresql, sqlite, sqlserver]

# GROUPINGS OF DATABASE TYPES.

uses_connection_string = set(db_types) - {mysql}
file_databases = {access, sqlite}

# DATABASE LIBRARIES.

cx_Oracle = 'cx_Oracle'
psycopg2 = 'psycopg2'
pymysql = 'pymysql'
pyodbc = 'pyodbc'
sqlite3 = 'sqlite3'
lib_name_for_db = {access: pyodbc, mysql: pymysql, oracle: cx_Oracle,
                   postgresql: psycopg2, sqlite: sqlite3, sqlserver: pyodbc}

# DATABASE CLIENT EXECUTABLES.  THEIR DIRECTORIES MUST BE IN PATH.

db_client_exes = {access: '', mysql: 'mysqlsh', oracle: 'sqlplus',
                  postgresql: 'psql', sqlite: 'sqlite3', sqlserver: 'sqlcmd'}

# PARAMETERIZATION/BIND VARIABLE FORMAT USED HERE.
named = 'named'
pyformat = 'pyformat'
qmark = 'qmark'
paramstyle_for_lib = {cx_Oracle: named, psycopg2: pyformat,
                      pymysql: pyformat, pyodbc: qmark, sqlite3: named}
