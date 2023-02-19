import psycopg2
import csv
# psycopg3 exists

# Make connection.
connection = psycopg2.connect("user='root' password='password' host='localhost' port='5432' dbname='ds2'")

cursor = connection.cursor()

# SELECT
results = cursor.execute("SELECT * FROM categories")
rows = cursor.fetchall()
for record in rows:
    print(record)

with open(file='../junk/whatever.txt', encoding="utf8", newline='') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter='\t')
    for line in csv_reader:
        if "'" in line[1]:
            line[1] = line[1].replace("'", "''")
        SQL = f'INSERT INTO whatever (id, name) VALUES ({line[0]}, \'{line[1]}\')'
        print(SQL)
        cursor.execute(SQL)

# Finish up.
connection.commit()
cursor.close()
connection.close()
