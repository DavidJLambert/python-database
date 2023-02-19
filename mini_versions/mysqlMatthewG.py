import pymysql
import csv

# Record commit frequency.
commit_frequency = 25

# MySQL database login and location information.
"""
username = 'root'
password = 'comp430'
hostname = '3.82.54.231'
port_num = 3306
instance = 'imdb'
"""
username = 'root'
password = 'password'
hostname = '127.0.0.1'
port_num = 3306
instance = 'DS2'

# Make connection.
connection = pymysql.connect(user=username, password=password, host=hostname, port=port_num, database=instance,
                                     autocommit=False)

# Create cursor.
cursor = connection.cursor()

# File to import.
filename = input("Enter name of file to import: ").strip()

# Use filename (minus extension) as table name.
table_name = filename.split(".")[0]

# See if this table already exists.
table_exists_select = (
    "SELECT COUNT(table_name)\n"
    "FROM information_schema.tables\n"
    "WHERE table_type = 'BASE TABLE'\n"
    "AND table_name = '%s'\n"
    "AND table_schema = '%s'")

sql = table_exists_select % (table_name, instance)
cursor.execute(sql)
rows = cursor.fetchone()
number_tables = int(rows[0])
if number_tables > 0:
    print("\nTable '%s' already exists, exiting.")
    exit(0)

with open(file=filename, newline='') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=",")
    # Read file 2 times.  The first read: get column data types and lengths.
    for line_number, line in enumerate(csv_reader):
        if line_number == 0:
            # The first line has column headings, which we'll make the column names.

            # Make column headings uppercase.
            columns = []
            for column in line:
                columns.append(column.upper())

            # Assume datatype is integer until proven otherwise.
            data_types = []
            for column in line:
                data_types.append("INTEGER")

            # Get ready to record data lengths of the columns.
            data_lengths = []
            for column in line:
                data_lengths.append(0)
        else:
            # The rest of the lines have data.
            for column_number, column in enumerate(line):
                # Remove minus sign from numbers, they interfere with date lengths and assigning data types.
                if len(column) > 0 and column[0] == "-":
                    column = column[1:]
                # Get the maximum data length.
                data_lengths[column_number] = max(len(column), data_lengths[column_number])
                # Get the data type.
                if data_types[column_number] != "VARCHAR" and column.isalpha():
                    data_types[column_number] = "VARCHAR"
                if data_types[column_number] == "INTEGER" and column.isdecimal():
                    data_types[column_number] = "DECIMAL"

    # Create the table.
    for column_number, column in enumerate(columns):
        if data_types[column_number] == "VARCHAR":
            columns[column_number] = "%s VARCHAR(%d)" % (column, data_lengths[column_number])
        else:
            columns[column_number] = "%s %s" % (column, data_types[column_number])
    sql = "CREATE TABLE " + table_name + " (\n" + ",\n".join(columns) + ")"
    print(sql)

'''
    # The second read: import
    for line_number, line in enumerate(csv_reader):
        if line_number > 0:
            # The rest of the lines have data.
'''
# Commit any remaining inserts.
connection.commit()

# Finish up.
cursor.close()
connection.close()

print("All Done.")
