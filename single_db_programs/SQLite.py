""" SQLite.py

SUMMARY: Demonstration of how DB-API 2.0 code can interact with a SQLite database.

REPOSITORY: https://github.com/DavidJLambert/Python-Universal-DB-Client

AUTHOR: David J. Lambert

DATE: Feb 18, 2023

For more information, see README.rst.
"""

import sqlite3

ts_dict = {0: 'Single-threaded', 1: 'Multi-threaded', 3: 'Serialized'}

# Connection constants
db_path = r"C:\Coding\PyCharm\projects\Python-Universal-DB-Client\databases\ds2.sqlite3"

# Properties of the sqlite3 library.
print(f"DB-API level: {sqlite3.apilevel}.")
print(f"DB Library Version: {sqlite3.version}/{sqlite3.version_info}.")
print(f"SQLite version: {sqlite3.sqlite_version}/{sqlite3.sqlite_version_info} (Not in DB-API 2.0).")
print(f"sqlite3 default parameter style: '{sqlite3.paramstyle}'. "
      f"\n\t'numeric' and 'named' supported in sqlite3,\n\t'format' and 'pyformat' not supported in sqlite3.")
ts = sqlite3.threadsafety
print(f"Thread safety: {ts} ('{ts_dict[ts]}').\n\tOptions: {ts_dict}")

# Make connection.
connection = sqlite3.connect(database=db_path, timeout=10, isolation_level='DEFERRED')
print("\nCreated connection.")

# Properties and methods of the connection object
print(f"Isolation level: '{connection.isolation_level}' (Not in DB-API 2.0).  "
      f"\n\tOptions: 'None' (autocommit), 'DEFERRED', 'IMMEDIATE' or 'EXCLUSIVE'.")

# Create cursor.
cursor = connection.cursor()
print("\nCreated cursor.")

# SELECT
SQL = "SELECT * FROM categories"
cursor.execute(SQL)
print(f"\nExecuted SQL: '{SQL}'")

columns = [item[0] for item in cursor.description]
columns = ', '.join(columns)
print(f"Columns in result set: '{columns}'")

# The result set.
rows = cursor.fetchall()
print("Rows in result set:")
for row in rows:
    print(row)
print(f"Uncommitted transactions: '{connection.in_transaction}' (Not in DB-API 2.0).")
print(f"Number of updated, inserted, or deleted rows: {connection.total_changes} (Not in DB-API 2.0).")
print(f"cursor.rowcount = {cursor.rowcount}.  (Not in DB-API 2.0)")

# SQL with Bind Variables.
# paramstyle = 'qmark' (default)
print(f"\nsqlite3 parameter style: '{sqlite3.paramstyle}'")
SQL = "SELECT * FROM Categories WHERE category = ? OR categoryname = ?"
cursor.execute(SQL, [1, "Drama"])
print(f"Executed SQL: '{SQL}',\n\twith bind variables 1 and 'Drama', respectively.")
rows = cursor.fetchall()
print("Rows in result set:")
for row in rows:
    print(row)

# paramstyle = 'named'.
sqlite3.paramstyle = 'named'
print(f"\nsqlite3 parameter style: '{sqlite3.paramstyle}'.")
SQL = "SELECT * FROM Categories WHERE category = :var1 OR categoryname = :var2"
cursor.execute(SQL, {"var1": 1, "var2": "Drama"})
print(f"Executed SQL: '{SQL}',\n\twith bind variables :var1 = 1 and :var2 = 'Drama'.")
print('Bind variables in dictionary.')
print('Rows in result set:')
rows = cursor.fetchall()
for row in rows:
    print(row)

print('Bind variables in list.')
print('Rows in result set:')
cursor.execute(SQL, [1, 'Drama'])
rows = cursor.fetchall()
for row in rows:
    print(row)

# paramstyle = 'numeric'.
sqlite3.paramstyle = 'numeric'
print(f"\nsqlite3 parameter style: '{sqlite3.paramstyle}'.")
SQL = "SELECT * FROM Categories WHERE category = :1 OR categoryname = :2"
cursor.execute(SQL, [1, 'Drama'])
print(f"Executed SQL: '{SQL}',\n\twith bind variables :1 = 1 and :2 = 'Drama'.")
print("Rows in result set:")
rows = cursor.fetchall()
for row in rows:
    print(row)

# Back to the default paramstyle.
sqlite3.paramstyle = 'qmark'

# SQL INSERT.
SQL = "INSERT INTO Categories (category, categoryname) VALUES (?, ?)"
cursor.execute(SQL, [20, 'Kumquats'])
print(f"\nExecuted SQL: '{SQL}',\n\twith bind variables 20 and 'Drama', respectively.")
print(f"Uncommitted transactions: '{connection.in_transaction}' (Not in DB-API 2.0).")
print(f"Number of updated, inserted, or deleted rows: {connection.total_changes} (Not in DB-API 2.0).")
print(f"cursor.rowcount = {cursor.rowcount}.  (Not in DB-API 2.0)")

# SQL UPDATE.
SQL = "UPDATE Categories SET categoryname = ? WHERE category = ?"
cursor.execute(SQL, ['zymurgy', 20])
print(f"\nExecuted SQL: '{SQL}',\n\twith bind variables 'zymurgy' and 20, respectively.")
print(f"Uncommitted transactions: '{connection.in_transaction}' (Not in DB-API 2.0).")
print(f"Number of updated, inserted, or deleted rows: {connection.total_changes} (Not in DB-API 2.0).")
print(f"cursor.rowcount = {cursor.rowcount}.  (Not in DB-API 2.0)")

# SQL DELETE.
SQL = "DELETE FROM Categories WHERE category = ?"
cursor.execute(SQL, [20])
print(f"\nExecuted SQL: '{SQL}',\n\twith bind variable 20.")
print(f"Uncommitted transactions: '{connection.in_transaction}' (Not in DB-API 2.0).")
print(f"Number of updated, inserted, or deleted rows: {connection.total_changes} (Not in DB-API 2.0).")
print(f"cursor.rowcount = {cursor.rowcount}.  (Not in DB-API 2.0)")

# Other options.
# cursor.rollback()
# connection.interrupt()

# Finish up.
print("\nCommitting.")
connection.commit()
print(f"Uncommitted transactions: '{connection.in_transaction}' (Not in DB-API 2.0).")
print(f"Number of updated, inserted, or deleted rows: {connection.total_changes} (Not in DB-API 2.0).")
print(f"cursor.rowcount = {cursor.rowcount}.  (Not in DB-API 2.0)")

cursor.close()
connection.close()
