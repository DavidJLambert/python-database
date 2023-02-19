import sqlite3
import csv
import tkinter as tk
from tkinter import filedialog

# Ask for file to import.
root = tk.Tk()
root.withdraw()
csv_file_path = filedialog.askopenfilename()
if len(csv_file_path) == 0:
    print("No file specified, quitting.")
    exit(1)
print(f"Importing file {csv_file_path}")

# Make connection to database.  If database does not exist, create it.
connection = sqlite3.connect(database='data.sqlite3')

# Create cursor.
cursor = connection.cursor()

# If table does not exist, create it.
create_sql = '''
CREATE TABLE IF NOT EXISTS deviceinfo (
analyzer_number INTEGER,
lora_device_number INTEGER,
wavelength INTEGER,
time INTEGER,
chamber1 INTEGER,
chamber2 INTEGER,
chamber3 INTEGER,
chamber4 INTEGER,
chamber5 INTEGER,
chamber6 INTEGER,
chamber7 INTEGER,
chamber8 INTEGER
)'''
cursor.execute(create_sql)

# Construct the SQL insert.
insert_values_bind = ["?"] * 12
insert_values_bind = "( " + ", ".join(insert_values_bind) + " )"
insert_sql = "INSERT INTO deviceinfo VALUES " + insert_values_bind

# Open import file.
delimiter = ","
with open(file=csv_file_path, encoding="utf8", newline='') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=delimiter)
    # Convert lines in import file into SQL inserts.
    line_number = 1
    for line in csv_reader:
        # Split the first column at the underscores.
        more_line = line[0].split("_")
        if len(line) != 8 or len(more_line) != 8:
            print(f"Bad number of columns, skipping line number {line_number}.")
        else:
            # DataMsg_006_254_Channel515nm_Time_39_Chambers1to8_8707,17370,10060,7763,5690,7864,6057,2339
            #       0   1   2            3    4  5            6    7     1     2    3    4    5    6    7
            analyzer_number = int(more_line[1])
            lora_device_number = int(more_line[2])
            wavelength = more_line[3]
            wavelength = int(wavelength[7:-2])
            time = int(more_line[5])
            chamber1 = int(more_line[7])
            chamber2 = int(line[1])
            chamber3 = int(line[2])
            chamber4 = int(line[3])
            chamber5 = int(line[4])
            chamber6 = int(line[5])
            chamber7 = int(line[6])
            chamber8 = int(line[7])

            columns = [analyzer_number, lora_device_number, wavelength, time,
                       chamber1, chamber2, chamber3, chamber4,
                       chamber5, chamber6, chamber7, chamber8]

            # Do the insert.
            cursor.execute(insert_sql, columns)

            # Increment line counter.
            line_number += 1

# Commit inserts.
connection.commit()

# Show all records in this table.
select_sql = "SELECT * FROM deviceinfo"
cursor.execute(select_sql)
for row in cursor.fetchall():
    print(row)

# Finish up.
cursor.close()
connection.close()

print("All Done.")
