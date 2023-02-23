import oracledb

# Connection constants
username = 'ds2'
password = 'ds2'
hostname = '127.0.0.1'
port_num = 1521
instance = 'XE'

# Properties of the psycopg2 library.
db_lib_name = 'psycopg2'
if db_lib_name in {'psycopg2', 'pymysql'}:
    db_lib_version = oracledb.__version__
else:
    db_lib_version = oracledb.version
print("DB Library Version:", db_lib_version)

print("oracledb parameter style ('named', 'qmark', or 'pyformat'):", oracledb.paramstyle)
# paramstyle = 'named': oracledb.  Option for sqlite3 & psycopg2.
# paramstyle = 'qmark': sqlite3 and pyodbc.
# paramstyle = 'pyformat': pymysql and psycopg2.

# Make connection.
connect_string = f"{username}/{password}@{hostname}:{port_num}/{instance}"
print(connect_string)
connection = oracledb.connect(connect_string)
print("Created connection.")

# Properties and methods of the connection object.
# connection.interrupt()

# Create cursor.
cursor = connection.cursor()
print("Created cursor.")

# SELECT
SQL = "SELECT * FROM Categories"
cursor.execute(SQL)
print(f"Executed SQL: {SQL}")

columns = [item[0] for item in cursor.description]
columns = ', '.join(columns)
print(f"Columns in result set: '{columns}'")

# The result set.
rows = cursor.fetchall()
print("Rows in result set:")
for row in rows:
    print(row)

# SQL with Bind Variables.

SQL = "SELECT * FROM Categories WHERE category = :var1 OR categoryname = :var2"
# Either format is acceptable.
cursor.execute(SQL, {"var1": 1, "var2": "Drama"})
# cursor.execute(SQL, [1, "Drama"])

# The result set.
rows = cursor.fetchall()
print("Rows in result set:")
for row in rows:
    print(row)

"""
# Properties and methods of the cursor object.
# cursor.rollback()
# cursor.connection
# print(cursor.rowcount), often -1 for psycopg2
"""
# Finish up.
connection.commit()
cursor.close()
connection.close()
