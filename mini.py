import sqlite3
# Constants
db_path = "C:\\Coding\\PyCharm\\projects\\Python-Universal-DB-Client\\databases\\ds2.sqlite3"
sql = "SELECT * FROM categories"

# Properties of the sqlite3 library.
db_lib_name = 'sqlite3'
if db_lib_name in {'psycopg2', 'pymysql'}:
    db_lib_version = sqlite3.__version__
else:
    db_lib_version = sqlite3.version
print("DB Library Version:", db_lib_version)
print("SQLite version:", sqlite3.sqlite_version)
print("SQLite parameter style ('named', 'qmark', or 'pyformat'):", sqlite3.paramstyle)

# Properties and methods of the connection object.
connection = sqlite3.connect(database=db_path, timeout=5.0, isolation_level=None)
# connection.interrupt()
print("Isolation level ('None' for autocommit, 'DEFERRED', 'IMMEDIATE' or 'EXCLUSIVE'):", connection.isolation_level)
cursor = connection.cursor()

# Properties and methods of the cursor object.
cursor.execute(sql)
# cursor.rollback()
# cursor.connection
# print(cursor.rowcount), often -1 for sqlite3
rows = cursor.fetchall()
column_names = [item[0] for item in cursor.description]
print(column_names)
cursor.close()

# Properties and methods of the result set.
for row in rows:
    print(row)

# More properties and methods of the connection object.
print("Uncommitted transactions:", connection.in_transaction)
print("Number of updated, inserted, or deleted rows on this connection:", connection.total_changes)
connection.commit()
connection.close()
