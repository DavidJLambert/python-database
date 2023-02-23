""" SQLite_import.py

SUMMARY: Demonstration of how DB-API 2.0 code can import a flat file into a SQLite database.

REPOSITORY: https://github.com/DavidJLambert/Python-Universal-DB-Client

AUTHOR: David J. Lambert

DATE: Feb 19, 2023

For more information, see README.rst.
"""

import sqlite3
import string
import csv
import tkinter as tk
from tkinter import filedialog

keep = string.ascii_lowercase + string.digits + "_ "
ts_dict = {0: 'Single-threaded', 1: 'Multi-threaded', 3: 'Serialized'}
commit_frequency = 25
table_exists_select = "SELECT COUNT(name) FROM sqlite_master WHERE type='table' AND name = ?"

# Get name & location of csv & DB files.  filedialog.askopenfilename is fragile, have to do this here.
title = 'Select the csv file to import.'
root = tk.Tk()
csv_file_path = filedialog.askopenfilename(title=title)
if csv_file_path == "":
    print("No csv file selected, exiting.")
    exit(1)

title = 'Select the SQLite DB to import into, or click the Cancel button to create a new DB.'
db_path = filedialog.askopenfilename(title=title)
root.destroy()
if db_path == "":
    new_db = True
    db_path = input("Enter the name of the new DB file to create: ")
else:
    new_db = False

# Get the column delimiter.
delimiter = input("Enter the delimiter used to separate columns (or 'tab' for tab): ").strip()
if delimiter.lower() == "tab":
    delimiter = "\t"

# Create new table, or use existing one?
if new_db:
    choice = '1'
else:
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
connection = sqlite3.connect(database=db_path, timeout=10, isolation_level='DEFERRED')
print("\nCreated connection.")

# Properties of the sqlite3 library.
print(f"DB-API level: {sqlite3.apilevel}.")
print(f"DB Library Version: {sqlite3.version}/{sqlite3.version_info}.")
print(f"SQLite version: {sqlite3.sqlite_version}/{sqlite3.sqlite_version_info} (Not in DB-API 2.0).")
print(f"sqlite3 default parameter style: '{sqlite3.paramstyle}'. "
      f"\n\t'numeric' and 'named' supported in sqlite3,\n\t'format' and 'pyformat' not supported in sqlite3.")
ts = sqlite3.threadsafety
print(f"Thread safety: {ts} ('{ts_dict[ts]}').\n\tOptions: {ts_dict}")

# Properties and methods of the connection object
print(f"Isolation level: '{connection.isolation_level}' (Not in DB-API 2.0).  "
      f"\n\tOptions: 'None' (autocommit), 'DEFERRED', 'IMMEDIATE' or 'EXCLUSIVE'.")

# Create cursor.
cursor = connection.cursor()
print("\nCreated cursor.")

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
                create_table_sql.append(table_name + "_pkey INTEGER PRIMARY KEY AUTOINCREMENT")
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

            insert_values_bind = ["?"]*len(column_list)
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
