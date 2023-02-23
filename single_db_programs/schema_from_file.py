""" schema_from_file.py

SUMMARY:
    Reads a file to be imported into a database,
    and constructs a SQL create table command to hold the data in that file.

INPUTS:
    1) File directory/name.
    2) Column separator character string.

REPOSITORY: https://github.com/DavidJLambert/Python-Universal-DB-Client

AUTHOR: David J. Lambert

DATE: Feb 22, 2023

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
                return "CHAR"
            else:
                print(f"Unknown data type {result} in get_datatype for {data}.")
                return "CHAR"
        except Exception:
            return "CHAR"


def combine_datatypes(type1, type2):
    if type1 == type2:
        return type1
    elif type1 == "CHAR" or type2 == "CHAR":
        return "CHAR"
    elif type1 == "INTEGER" and type2 == "FLOAT":
        return "FLOAT"
    elif type1 == "FLOAT" and type2 == "INTEGER":
        return "FLOAT"
    elif type1 == "BOOLEAN" and type2 != "BOOLEAN":
        return "CHAR"
    elif type1 != "BOOLEAN" and type2 == "BOOLEAN":
        return "CHAR"
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

# Get DB type.
while True:
    prompt = "Enter 'O' for Oracle, 'S' for SQL Server, 'P' for PostgreSQL, 'M' for MySQL, or 'L' for SQLite: "
    db_type = input(prompt)[0].upper()
    if db_type in ('O', 'S', 'P', 'M', 'L'):
        break

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
    max_char_len = [0] * number_columns
    min_char_len = [8000] * number_columns  # 8000 for SQL Server.  Oracle, PostgreSQL & MySQL max is 65,535.
    max_r_float = [0] * number_columns  # p and s take their names from NUMERIC(p, s), r = p - s.
    max_s_float = [0] * number_columns
    max_int = [0] * number_columns  # only tracking absolute value

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

            # Possibly update data type.
            data_types[column_number] = combine_datatypes(datatype, data_types[column_number])

            # Nullability.
            if datatype == "NULL":
                nullability[column_number] = True

            # Update data lengths.
            if datatype == "CHAR":
                max_char_len[column_number] = max(len(column), max_char_len[column_number])
                min_char_len[column_number] = min(len(column), min_char_len[column_number])
            elif datatype == "INTEGER":
                column = abs(int(column))
                max_int[column_number] = max(column, max_int[column_number])
            elif datatype == "FLOAT":
                int_part_float, frac_part_float = column.split(".")

                # strip minus sign from front of int_part_float
                if int_part_float[0] == "-":
                    int_part_float = int_part_float[1:]

                # strip zeroes from end of frac_part_float
                while frac_part_float[-1] == "0":
                    frac_part_float = frac_part_float[:-1]

                max_r_float[column_number] = max(len(int_part_float), max_r_float[column_number])
                max_s_float[column_number] = max(len(frac_part_float), max_s_float[column_number])

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

    # INT, CHAR DONE
    # Assemble column specifications.
    max_p_float = max_r_float[col_num] + max_s_float[col_num]

    # CHAR the same for all database types.
    if data_types[col_num] == "CHAR":
        if max_char_len[col_num] == min_char_len[col_num]:
            this_data_type = f"CHAR({max_char_len[col_num]})"
        elif db_type == "O":
            this_data_type = f"VARCHAR2({max_char_len[col_num]})"
        else:
            this_data_type = f"VARCHAR({max_char_len[col_num]})"

        SQL.append(f"{column_names[col_num]} {this_data_type}{nullable}")
    elif data_types[col_num] == "FLOAT":
        SQL.append(f"{column_names[col_num]} NUMERIC({max_p_float}, {max_s_float[col_num]}){nullable}")

    if db_type == "O":
        if data_types[col_num] == "INTEGER":
            SQL.append(f"{column_names[col_num]} INTEGER{nullable}")
        elif data_types[col_num] not in ("CHAR", "FLOAT"):
            SQL.append(f"{column_names[col_num]} {data_types[col_num]}{nullable}")
    elif db_type == "S":
        if data_types[col_num] == "INTEGER":
            if max_int[col_num] <= 255:
                SQL.append(f"{column_names[col_num]} TINYINT{nullable}")
            elif max_int[col_num] <= 32_767:
                SQL.append(f"{column_names[col_num]} SMALLINT{nullable}")
            elif max_int[col_num] <= 2_147_483_647:
                SQL.append(f"{column_names[col_num]} INT{nullable}")
            elif max_int[col_num] <= 9_223_372_036_854_775_807:
                SQL.append(f"{column_names[col_num]} BIGINT{nullable}")
            else:
                SQL.append(f"{column_names[col_num]} TOO BIG INT{nullable}")
        elif data_types[col_num] not in ("CHAR", "FLOAT"):
            SQL.append(f"{column_names[col_num]} {data_types[col_num]}{nullable}")
    elif db_type == "P":
        if data_types[col_num] == "INTEGER":
            if max_int[col_num] <= 32_767:
                SQL.append(f"{column_names[col_num]} SMALLINT{nullable}")
            elif max_int[col_num] <= 2_147_483_647:
                SQL.append(f"{column_names[col_num]} INT{nullable}")
            elif max_int[col_num] <= 9_223_372_036_854_775_807:
                SQL.append(f"{column_names[col_num]} BIGINT{nullable}")
            else:
                SQL.append(f"{column_names[col_num]} TOO BIG INT{nullable}")
        elif data_types[col_num] not in ("CHAR", "FLOAT"):
            SQL.append(f"{column_names[col_num]} {data_types[col_num]}{nullable}")
    elif db_type == "M":
        if data_types[col_num] == "INTEGER":
            if max_int[col_num] <= 32_767:
                SQL.append(f"{column_names[col_num]} SMALLINT{nullable}")
            elif max_int[col_num] <= 8_388_607:
                SQL.append(f"{column_names[col_num]} MEDIUMINT{nullable}")
            elif max_int[col_num] <= 2_147_483_647:
                SQL.append(f"{column_names[col_num]} INT{nullable}")
            elif max_int[col_num] <= 9_223_372_036_854_775_807:
                SQL.append(f"{column_names[col_num]} BIGINT{nullable}")
            else:
                SQL.append(f"{column_names[col_num]} TOO BIG INT{nullable}")
        elif data_types[col_num] not in ("CHAR", "FLOAT"):
            SQL.append(f"{column_names[col_num]} {data_types[col_num]}{nullable}")
    elif db_type == "L":
        if data_types[col_num] == "INTEGER":
            if max_int[col_num] <= 255:
                SQL.append(f"{column_names[col_num]} TINYINT{nullable}")
            elif max_int[col_num] <= 32_767:
                SQL.append(f"{column_names[col_num]} SMALLINT{nullable}")
            elif max_int[col_num] <= 8_388_607:
                SQL.append(f"{column_names[col_num]} MEDIUMINT{nullable}")
            elif max_int[col_num] <= 2_147_483_647:
                SQL.append(f"{column_names[col_num]} INT{nullable}")
            elif max_int[col_num] <= 9_223_372_036_854_775_807:
                SQL.append(f"{column_names[col_num]} BIGINT{nullable}")
            else:
                SQL.append(f"{column_names[col_num]} TOO BIG INT{nullable}")
        elif data_types[col_num] not in ("CHAR", "FLOAT"):
            SQL.append(f"{column_names[col_num]} {data_types[col_num]}{nullable}")

# Assemble entire statement.
SQL = ',\n '.join(SQL)
SQL = f"CREATE TABLE {table_name} \n({SQL}\n)"

print()
print(SQL)
