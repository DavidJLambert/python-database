""" SQLite_schema_from_file.py

SUMMARY:
    Reads a file to be imported into a database,
    and constructs a SQL create table command to hold the data in that file.

INPUTS:
    1) File directory/name.
    2) Column separator character string.

REPOSITORY: https://github.com/DavidJLambert/Python-Universal-DB-Client

AUTHOR: David J. Lambert

VERSION: 0.1.0

DATE: Feb 18, 2023

For more information, see README.rst.
"""
import json
import string
import csv
import tkinter as tk
from tkinter import filedialog

keep = string.ascii_lowercase + string.digits + "_ "


def get_datatype(data):
    if data is None:
        return "NULL"
    elif len(data) == 0:
        return "NULL"
    else:
        try:
            result = type(json.loads(data.lower()))
            if result == bool:
                return "BOOLEAN"
            elif result == int:
                return "INTEGER"
            elif result == float:
                return "FLOAT"
            elif result == str:
                return "VARCHAR"
            else:
                print(f"Unknown data type {result} in get_datatype for {data}.")
                return "VARCHAR"
        except Exception:
            return "VARCHAR"


def combine_datatypes(type1, type2):
    if type1 == type2:
        return type1
    elif type1 == "VARCHAR" or type2 == "VARCHAR":
        return "VARCHAR"
    elif type1 == "INTEGER" and type2 == "FLOAT":
        return "FLOAT"
    elif type1 == "FLOAT" and type2 == "INTEGER":
        return "FLOAT"
    elif type1 == "BOOLEAN" and type2 != "BOOLEAN":
        return "VARCHAR"
    elif type1 != "BOOLEAN" and type2 == "BOOLEAN":
        return "VARCHAR"
    elif type1 == "NULL":
        return type2
    elif type2 == "NULL":
        return type1
    else:
        raise Exception(f"Unknown combination of data types: {type1}, {type2}")


# Get name and location of csv file.
root = tk.Tk()
root.withdraw()
csv_file_path = filedialog.askopenfilename()
print(f"Analyzing {csv_file_path}")

# Get column delimiter.
delimiter = input("Enter the delimiter used to separate columns (or 'tab' for tab): ").strip()
if delimiter.lower() == "tab":
    delimiter = "\t"

# Read file.
with open(file=csv_file_path, encoding="utf8", newline='') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=delimiter)
    # Get first line of the file.
    column_names = next(csv_reader)

    columns = []
    for column in column_names:
        column = column.lower().replace("#", "num")
        new_column = ""
        for char in column:
            if char in keep:
                new_column += char
        column = new_column.replace(" ", "_")
        columns.append(column)
    column_names = columns

    number_columns = len(columns)

    # Initialize data types and data lengths.
    data_types = ["NULL"] * number_columns
    nullability = [False] * number_columns
    max_data_len = [0] * number_columns
    min_data_len = [8000] * number_columns # for SQL Server.  Oracle, postgreSQL, & MySQL max is 65,535.

    # Iterate through all lines.
    for line_number, line in enumerate(csv_reader):
        if len(line) > number_columns:
            print(f"The number of columns is not constant.")
            print("Edit the file to fix this.  See line {line_number-1}.")
        # Iterate through all columns.
        for column_number, column in enumerate(line):
            column = column.strip()
            # Data type of current value.
            datatype = get_datatype(column)

            # Nullability.
            if datatype == "NULL":
                nullability[column_number] = True

            # Update data lengths.
            max_data_len[column_number] = max(len(column), max_data_len[column_number])
            min_data_len[column_number] = min(len(column), min_data_len[column_number])

            # Possibly update data type.
            data_types[column_number] = combine_datatypes(datatype, data_types[column_number])

# Get table name.
table_name = input("Enter the name of the table to create and import into: ").strip().lower()

# Write tentative SQL Create statement.
SQL = []

# Get columns and their specifications.
for col_num in range(number_columns):
    # Nullability.
    if nullability[col_num]:
        nullable = ""
    else:
        nullable = " NOT NULL"
    # Assemble column specifications.
    if data_types[col_num] == "VARCHAR":
        if max_data_len[col_num] == min_data_len[col_num]:
            this_data_type = f"CHAR({max_data_len[col_num]})"
        else:
            this_data_type = f"VARCHAR({max_data_len[col_num]})"
        SQL.append(f"{column_names[col_num]} {this_data_type}{nullable}")
    else:
        SQL.append(f"{column_names[col_num]} {data_types[col_num]}{nullable}")

# Assemble entire statement.
SQL = ',\n '.join(SQL)
SQL = f"CREATE TABLE {table_name} \n({SQL})"

print()
print(SQL)
