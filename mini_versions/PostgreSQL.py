import psycopg2
# psycopg3 exists
# pip install psycopg2

# Connection constants
username = 'ds2'
password = 'ds2'
hostname = '127.0.0.1'
port_num = 5432
instance = 'ds2'

# Properties of the psycopg2 library.
db_lib_name = 'psycopg2'
if db_lib_name in {'psycopg2', 'pymysql'}:
    db_lib_version = psycopg2.__version__
else:
    db_lib_version = psycopg2.version
print("DB Library Version:", db_lib_version)

print("psycopg2 parameter style ('named', 'qmark', or 'pyformat'):", psycopg2.paramstyle)
# paramstyle = 'named': oracledb.  Option for sqlite3 & psycopg2.
# paramstyle = 'qmark': sqlite3 and pyodbc.
# paramstyle = 'pyformat': pymysql and psycopg2.

# Make connection.
connect_string = f"user='{username}' password='{password}' host='{hostname}' port='{port_num}' dbname='{instance}'"
connection = psycopg2.connect(connect_string)
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
SQL = "SELECT * FROM Categories WHERE category = %(var1)s OR categoryname = %(var2)s"
cursor.execute(SQL, {"var1": 1, "var2": "Drama"})
# Or positional format
# SQL = "SELECT * FROM Categories WHERE category = %s OR categoryname = %s"
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
