""" PostgreSQL.py

SUMMARY: Demonstration of how DB-API 2.0 code can interact with a PostgreSQL database.

REPOSITORY: https://github.com/DavidJLambert/Python-Universal-DB-Client

AUTHOR: David J. Lambert

DATE: Feb 19, 2023

For more information, see README.rst.
"""

import psycopg

# Connection constants
username = 'ds2'
password = 'ds2'
hostname = '127.0.0.1'
port_num = 5432
instance = 'ds2'

# Properties of the psycopg3 library.
print(f"DB-API level: {psycopg.apilevel}.")
print(f"DB Library Version: {psycopg.__version__}.")
# Default paramstyle 'pyformat'.
print(f"psycopg default parameter style: '{psycopg.paramstyle}'. "
      f"\n\t'format' supported in psycopg3,\n\t'qmark', 'named', and 'numeric' not supported.")
print(f"Thread safety: {psycopg.threadsafety}")

# Make connection.
connect_string = f"user='{username}' password='{password}' host='{hostname}' port='{port_num}' dbname='{instance}'"
connection = psycopg.connect(connect_string)
print("\nCreated connection.")

# Properties of the connection object.
print(f"Isolation level: '{connection.isolation_level}' (Not in DB-API 2.0)."
      f"\n\tOptions: 'None' (autocommit), 'DEFERRED', 'IMMEDIATE' or 'EXCLUSIVE'.")

# Create cursor.
cursor = connection.cursor()
print("\nCreated cursor.")

# PostgreSQL version.
SQL = "SELECT VERSION()"
cursor.execute(SQL)
print(f"PostgreSQL Server information: {cursor.fetchone()[0]}.")

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
print(f"cursor.rowcount = {cursor.rowcount}.  (Not in DB-API 2.0)")

# SQL with Bind Variables.
# paramstyle = 'pyformat' (default)
print(f"\nPostgreSQL parameter style: '{psycopg.paramstyle}'")
SQL = "SELECT * FROM Categories WHERE category = %(var1)s OR categoryname = %(var2)s"
cursor.execute(SQL, {"var1": 1, "var2": "Drama"})
print(f"Executed SQL: '{SQL}',\n\twith bind variables var1 = 1 and var2 = 'Drama'.")
print('Bind variables in dictionary.')
print('Rows in result set:')
rows = cursor.fetchall()
for row in rows:
    print(row)

# paramstyle = 'format'
psycopg.paramstyle = 'format'
print(f"\npsycopg3 parameter style: '{psycopg.paramstyle}'.")
SQL = "SELECT * FROM Categories WHERE category = %s OR categoryname = %s"
cursor.execute(SQL, [1, "Drama"])
print(f"Executed SQL: '{SQL}',\n\twith bind variables 1 (category) and 'Drama' (categoryname).")
print('Bind variables in list.')
print('Rows in result set:')
rows = cursor.fetchall()
for row in rows:
    print(row)

# Back to the default paramstyle.
psycopg.paramstyle = 'pyformat'

# SQL INSERT.
SQL = "INSERT INTO Categories (category, categoryname) VALUES (%(var1)s, %(var2)s)"
cursor.execute(SQL, {"var1": 20, "var2": "Kumquats"})
print(f"\nExecuted SQL: '{SQL}',\n\twith bind variables var1 = 20 and var2 = 'Drama'.")
print(f"cursor.rowcount = {cursor.rowcount}.  (Not in DB-API 2.0)")

# SQL UPDATE.
SQL = "UPDATE Categories SET categoryname = %(var1)s WHERE category = %(var2)s"
cursor.execute(SQL, {"var1": 'zymurgy', "var2": 20})
print(f"\nExecuted SQL: '{SQL}',\n\twith bind variables var1 = 'zymurgy' and var2 = 20.")
print(f"cursor.rowcount = {cursor.rowcount}.  (Not in DB-API 2.0)")

# SQL DELETE.
SQL = "DELETE FROM Categories WHERE category = %(var1)s"
cursor.execute(SQL, {"var1": 20})
print(f"\nExecuted SQL: '{SQL}',\n\twith bind variable var1 = 20.")
print(f"cursor.rowcount = {cursor.rowcount}.  (Not in DB-API 2.0)")

# Other options.
# cursor.rollback()
# connection.interrupt()

# Finish up.
print("\nCommitting.")
connection.commit()
print(f"cursor.rowcount = {cursor.rowcount}.  (Not in DB-API 2.0)")

cursor.close()
connection.close()
