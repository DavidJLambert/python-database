""" PosgreSQL_import.py

SUMMARY: Demonstration of how DB-API 2.0 code can import a flat file into a PosgreSQL database.

REPOSITORY: https://github.com/DavidJLambert/Python-Universal-DB-Client

AUTHOR: David J. Lambert

DATE: Feb 19, 2023

For more information, see README.rst.
"""

import psycopg
import string
import csv
import tkinter as tk
from tkinter import filedialog

keep = string.ascii_lowercase + string.digits + "_ "
commit_frequency = 25
table_exists_select = ("SELECT COUNT(*) FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND "
                       "table_schema NOT IN ('information_schema', 'pg_catalog') AND table_name = %s")

# Connection constants
username = 'ds2'
password = 'ds2'
hostname = '127.0.0.1'
port_num = 5432
instance = 'ds2'

# Get name & location of csv file.  filedialog.askopenfilename is fragile, have to do this here.
title = 'Select the csv file to import.'
root = tk.Tk()
csv_file_path = filedialog.askopenfilename(title=title)
root.destroy()
if csv_file_path == "":
    print("No csv file selected, exiting.")
    exit(1)

# Get the column delimiter.
delimiter = input("Enter the delimiter used to separate columns (or 'tab' for tab): ").strip()
if delimiter.lower() == "tab":
    delimiter = "\t"

# Create new table, or use existing one?
choice = ""
while choice not in ('1', '2'):
    choice = input("Enter '1' to create a table and import into it.\n"
                   "Enter '2' to import into an existing table: ")
new_table = (choice == '1')

# Enter SQL for create table, or auto-create from csv file column headings.
if new_table:
    choice = ""
    while choice not in ('A', 'B'):
        choice = input("\nYou are creating a new table.\n"
                       "Enter 'A' if you will enter the SQL to create it.\n"
                       "Enter 'B' to have 1 autoincrement primary key,\n"
                       "1 varchar column per csv file column,\n"
                       "with the varchar column names taken from the csv column headings: ")
    enter_SQL = (choice == 'A')
else:
    enter_SQL = False

# Make connection to database.
connect_string = f"user='{username}' password='{password}' host='{hostname}' port='{port_num}' dbname='{instance}'"
connection = psycopg.connect(connect_string)
print("\nCreated connection.")

# Properties of the psycopg3 library.
print(f"DB-API level: {psycopg.apilevel}.")
print(f"DB Library Version: {psycopg.__version__}.")
# Set paramstyle 'format'.
psycopg.paramstyle = 'format'
print(f"psycopg default parameter style: '{psycopg.paramstyle}'. "
      f"\n\t'format' supported in psycopg3,\n\t'qmark', 'named', and 'numeric' not supported.")
print(f"Thread safety: {psycopg.threadsafety}")

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

if not enter_SQL:
    table_name = input("Enter the name of the table to import into: ").strip().lower()

    # See if this table already exists.
    cursor.execute(table_exists_select, (table_name,))
    rows = cursor.fetchall()
    table_exists = (int(rows[0][0]) != 0)
else:
    print("Enter the create table SQL (you can paste multiple lines), then hit 'Enter' twice.")
    SQL = []
    while True:
        SQL_part = input()
        if SQL_part == "":
            break
        else:
            SQL.append(SQL_part)

    SQL = "\n".join(SQL)
    table_name = SQL.split("(")[0]
    table_name = table_name.split()[-1]

    # See if this table already exists.
    cursor.execute(table_exists_select, (table_name,))
    rows = cursor.fetchall()
    table_exists = (int(rows[0][0]) != 0)
    if table_exists:
        print("That table already exists.  Exiting.")
        exit(1)

# Execute CREATE TABLE statement.
if enter_SQL:
    cursor.execute(SQL)
    connection.commit()

with open(file=csv_file_path, encoding="utf8", newline='') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=delimiter)
    for line_number, row in enumerate(csv_reader):
        if line_number == 0:
            # Make column headings uppercase.

            if new_table and not enter_SQL:
                # Construct CREATE TABLE statement.
                column_list = []
                create_table_sql = []
                create_table_sql.append(table_name + "_pkey SERIAL PRIMARY KEY")
                for column in row:
                    column = column.lower().replace("#", "num")
                    new_column = ""
                    for char in column:
                        if char in keep:
                            new_column += char
                    column = new_column.replace(" ", "_")
                    create_table_sql.append(column + " TEXT")
                    column_list.append(column)
                create_table_sql = ',\n '.join(create_table_sql)
                create_table_sql = f"CREATE TABLE {table_name}\n({create_table_sql})"

                print("\nSQL for creating the table:\n" + create_table_sql)

                # Execute CREATE TABLE statement.
                cursor.execute(create_table_sql)
                connection.commit()
            else:
                # Get names of columns of existing table.
                cursor.execute(f"PRAGMA table_info({table_name})")
                column_list = []
                rows = cursor.fetchall()
                for row in rows:
                    column_list.append(row[1])

            # Construct bind variables expression.
            columns = ", ".join(column_list)

            insert_values_bind = ["%s"]*len(column_list)
            insert_values_bind = "( " + ", ".join(insert_values_bind) + " )"

            # Construct the SQL insert.
            insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES {insert_values_bind}"

            print("\nSQL for inserting records:\n" + insert_sql)
        else:
            # The remaining lines of the file contain column values.
            # Construct list of values to be inserted.
            values_list = []
            for column in row:
                values_list.append(column)
            values_tuple = tuple(values_list)

            # Do the insert using bind variables.
            cursor.execute(insert_sql, values_tuple)
            if line_number % commit_frequency == 0:
                connection.commit()
            if line_number % (10*commit_frequency) == 0:
                print(f"Inserted {line_number} records.")

# Commit any remaining inserts.
connection.commit()
print(f"Inserted a total of {line_number} records.")

# Finish up.
cursor.close()
connection.close()