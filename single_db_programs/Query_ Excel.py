""" Query_Excel.py

SUMMARY:
    Imports a csv file into a SQLite database.

INPUTS:
    1) File directory/name.

REPOSITORY: https://github.com/DavidJLambert/Python-Universal-DB-Client

AUTHOR: David J. Lambert

VERSION: 0.1.0

DATE: Oct 21, 2022
"""
import csv
import sqlite3
import os
from timeit import default_timer as timer
import tkinter as tk
from tkinter import filedialog

# Constants:
DELIMITER = ','
HEADERS = True
DB_FILE = 'temp.sqlite3'
COMMIT_FREQUENCY = 25

# Get name and location of csv file.
root = tk.Tk()
root.withdraw()
csv_file_path = filedialog.askopenfilename()

# Start timer.
start_time = timer()
# Connect to database.
connection = sqlite3.connect(database=DB_FILE)
# Create cursor.
cursor = connection.cursor()

if HEADERS:
    # Open csv file.
    with open(file=csv_file_path, encoding="utf8", newline='') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=DELIMITER)

        # Read first row (column headings) from csv file.
        row = next(csv_reader)
        # Get number of columns in csv file.
        number_columns = len(row)
        # Get column headings and generate CREATE TABLE statement.
        column_list = []
        for column in row:
            column_list.append(column + " text")
        column_list = ',\n\t'.join(column_list)
        CREATE_TABLE_SQL = "CREATE TABLE storage (\n\t" + column_list + "\n)"
        # Execute CREATE TABLE statement.
        cursor.execute(CREATE_TABLE_SQL)
        connection.commit()

        # Create parameterized SQL insert statement.
        value_list = ','.join('?'*number_columns)
        INSERT_SQL = 'INSERT INTO storage VALUES (' + value_list + ')'

        # Read the rest of the rows from the csv file, insert into database.
        for row_number, row in enumerate(csv_reader):
            cursor.execute(INSERT_SQL, row)
            if row_number % COMMIT_FREQUENCY == 0:
                connection.commit()

# Commit any remaining inserts.
connection.commit()

# Execute SQL and print results.
SQL = 'SELECT COUNT(*) FROM storage'
cursor.execute(SQL)
for row in cursor.fetchall():
    print(row)

# Disconnect from database.
cursor.close()
connection.close()

# Delete database file.
os.remove(DB_FILE)

# Stop timer.
elapsed_time = round(timer() - start_time, 2)
# Show how long the automatic part took.
print(f'{elapsed_time} seconds.')

print("ALL DONE.")
