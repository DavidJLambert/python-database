import pyodbc

# Connection constants
username = 'sa'
password = 'Pa$$w0rd'
hostname = '127.0.0.1'
port_num = 1433
instance = 'DS3'

# Properties of the pyodbc library.
db_lib_name = 'pyodbc'
if db_lib_name in {'psycopg2', 'pymysql'}:
    db_lib_version = pyodbc.__version__
else:
    db_lib_version = pyodbc.version
print("DB Library Version:", db_lib_version)

print("pyodbc parameter style ('named', 'qmark', or 'pyformat'):", pyodbc.paramstyle)

# Make connection.
connect_string = 'DRIVER={{SQL Server}};UID={};PWD={};SERVER={};PORT={};DATABASE={}'
connect_string = connect_string.format(username, password, hostname, port_num, instance)
connection = pyodbc.connect(connect_string, timeout=10, autocommit=False)

# Properties and methods of the connection object.
# connection.interrupt()

# Create cursor.
cursor = connection.cursor()

# SELECT
sql = "SELECT * FROM categories"
cursor.execute(sql)
# The result set.
rows = cursor.fetchall()
column_names = [item[0] for item in cursor.description]
print(column_names)
for row in rows:
    print(row)

# INSERT
sql = "INSERT INTO categories (CategoryName) VALUES ('Kumquats')"
cursor.execute(sql)
# print("Uncommitted transactions:", connection.in_transaction)
# print("Number of updated, inserted, or deleted rows on this connection:", connection.total_changes)
print("Committing transaction.")
connection.commit()
# print("Uncommitted transactions:", connection.in_transaction)
# print("Number of updated, inserted, or deleted rows on this connection:", connection.total_changes)

# Properties and methods of the cursor object.
# cursor.rollback()
# cursor.connection
# print(cursor.rowcount), often -1 for pyodbc

# Finish up.
connection.commit()
cursor.close()
connection.close()
