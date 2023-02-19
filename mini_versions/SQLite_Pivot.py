import sqlite3

# Connection constants
db_path = r"C:\Coding\PyCharm\projects\Python-Universal-DB-Client\databases\ds2.sqlite3"

# Properties of the sqlite3 library.
db_lib_name = 'sqlite3'
if db_lib_name in {'psycopg2', 'pymysql'}:
    db_lib_version = sqlite3.__version__
else:
    db_lib_version = sqlite3.version
print(f"DB Library Version: '{db_lib_version}'")

print(f"SQLite version: '{sqlite3.sqlite_version}'", )  # Not in DB-API 2.0
print(f"sqlite3 default parameter style: '{sqlite3.paramstyle}'")

# Make connection.
connection = sqlite3.connect(database=db_path, timeout=10, isolation_level='DEFERRED')
print("Created connection.")

# Properties and methods of the connection object
print(f"Isolation level ('None' for autocommit, 'DEFERRED', 'IMMEDIATE' or 'EXCLUSIVE'): '{connection.isolation_level}'")  # Not in DB-API 2.0

# Create cursor.
cursor = connection.cursor()
print("Created cursor.")

# SELECT
SQL = "SELECT * FROM categories"
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

print(f"Uncommitted transactions: '{connection.in_transaction}'", )  # Not in DB-API 2.0?
print(f"Number of updated, inserted, or deleted rows on this connection: '{connection.total_changes}'")  # Not in DB-API 2.0?
# connection.interrupt()

# SQL with Bind Variables.
# paramstyle = 'qmark' (default)
print(f"sqlite3 parameter style: '{sqlite3.paramstyle}'")
SQL = "SELECT * FROM Categories WHERE category = ? OR categoryname = ?"
cursor.execute(SQL, [1, "Drama"])
rows = cursor.fetchall()
print("Rows in result set:")
for row in rows:
    print(row)

# paramstyle = 'named'.
sqlite3.paramstyle = 'named'
print(f"sqlite3 parameter style: '{sqlite3.paramstyle}'")
SQL = "SELECT * FROM Categories WHERE category = :var1 OR categoryname = :var2"
# Bind variables in dictionary
cursor.execute(SQL, {"var1": 1, "var2": "Drama"})
rows = cursor.fetchall()
print("Rows in result set:")
for row in rows:
    print(row)

# Bind variables in list
cursor.execute(SQL, [1, "Drama"])
rows = cursor.fetchall()
print("Rows in result set:")
for row in rows:
    print(row)

# paramstyle = 'numeric'.
sqlite3.paramstyle = 'numeric'
print(f"sqlite3 parameter style: '{sqlite3.paramstyle}'")
SQL = "SELECT * FROM Categories WHERE category = :1 OR categoryname = :2"
cursor.execute(SQL, [1, "Drama"])
rows = cursor.fetchall()
print("Rows in result set:")
for row in rows:
    print(row)

# Properties and methods of the cursor object.
# cursor.rollback()
# cursor.connection
# print(cursor.rowcount), often -1 for sqlite3

# Finish up.
connection.commit()
cursor.close()
connection.close()
