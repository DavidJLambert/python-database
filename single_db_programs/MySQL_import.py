import csv
import os.path
import pymysql

# Record commit frequency.
commit_frequency = 25

# MySQL database login and location information.
print("Which MySQL database do you want to import your tsv or csv file into?")
while True:
    username = input("\nEnter username: ").strip()
    password = input("Enter password: ").strip()
    hostname = input("Enter hostname (DNS name or IP address): ").strip()
    port_num = input("Enter port number (default 3306): ").strip()
    if port_num.isdigit():
        port_num = int(port_num)
    else:
        print("Invalid port number: %s, starting over." % port_num)
        continue
    instance = input("Enter database/instance name: ").strip()
    prompt = '\nUsername = "%s"\nPassword = "%s"\nHostname = "%s"\nPort Number = "%d"\nDatabase/instance = "%s"\n'
    prompt += 'If this is not correct, press "return", otherwise enter any other character to accept: '
    prompt = prompt % (username, password, hostname, port_num, instance)
    response = input(prompt)
    if response != "":
        break

# Print properties of the pymysql library.
print(f"\npymysql library version: {pymysql.__version__}")
print(f"pymysql parameter style ('named', 'qmark', or 'pyformat'): {pymysql.paramstyle}")

# Make connection to database.
connection = pymysql.connect(user=username, password=password, host=hostname, port=port_num, database=instance)

# Create cursor.
cursor = connection.cursor()

# Prepare to get the name of the table to import into.
table_exists_select = (
    "SELECT COUNT(table_name)\n"
    "FROM information_schema.tables\n"
    "WHERE table_type = 'BASE TABLE'\n"
    "AND table_name = '%s'\n"
    "AND table_schema = '%s'")

# Get the name of the table to import into.
while True:
    print("\nWhich table do you want to import into?  If it does not exist, this program creates a new table")
    print("\nwith one varchar field for each column you import from your file.")
    print("\nThese fields will have the same name as the column headers of the columns you import.")
    table_name = input("Enter the table name: ").strip().upper()

    # See if this table already exists.
    sql = table_exists_select % (table_name, instance)
    cursor.execute(sql)
    rows = cursor.fetchall()
    table_exists = (int(rows[0][0]) == 0)

# File to import.
while True:
    filename = input("Enter full path of file to read: ").strip()
    if os.path.exists(filename):
        break
    else:
        print("%s does not exist, please try again." % filename)

# Get column delimiter.
# delimiter = ","
delimiter = input("Enter the delimiter used to separate columns (or 'tab' for tab): ").strip()
if delimiter.lower() == "tab":
    delimiter = "\t"

# Is the first line in the file column-headers?
while True:
    # line1_column_names = "Y"
    line1_column_names = input("Does the first line have column names? (Y/N): ").strip()
    if line1_column_names.upper() in ("Y", "N"):
        line1_column_names = (line1_column_names.upper() == "Y")
        break
    else:
        print("%s is an invalid choice, please try again." % line1_column_names)

with open(file=filename, encoding="utf8", newline='') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=delimiter)
    for line_number, all_columns_list in enumerate(csv_reader):
    for all_columns_list in csv_reader:
            if line_number == 0:
            # Make column headings uppercase.
            for column_number, column_text in enumerate(all_columns_list):
                all_columns_list[column_number] = column_text.upper()

            # Print list of columns from which to choose to import.
            prompt = "\nColumn    Column\nNumber    Header\n------    ----------"
            for column_number, column_text in enumerate(all_columns_list):
                prompt += "\n%6d    %s" % (column_number, column_text)
            prompt += "\n------    ----------"
            prompt += "\nEnter a comma-separated list of the column numbers you want to import: "

            # Choose columns to import.
            chosen_col_num_list2 = input(prompt).split(',')
            print()
            chosen_col_num_list = []
            for chosen_col_num in chosen_col_num_list2:
                chosen_col_num = int(chosen_col_num.strip())
                if chosen_col_num > len(all_columns_list) - 1:
                    print("%d is beyond the end of the list of columns!" % chosen_col_num)
                    exit(1)
                else:
                    chosen_col_num_list.append(chosen_col_num)
            # Remove duplicates from list.
            chosen_col_num_list = list(set(chosen_col_num_list))
            chosen_col_num_list.sort()

            # Construct CREATE TABLE statement.
            # table_name = filename.split('.')[0].upper()
            primary_key_name = table_name + '_PKEY'
            create_table_sql = "CREATE TABLE `%s` (" % table_name
            create_table_sql += "\n`%s` int(11) NOT NULL AUTO_INCREMENT" % primary_key_name
            for chosen_col_num in chosen_col_num_list:
                create_table_sql += ",\n`%s` varchar(1024)" % all_columns_list[chosen_col_num]
            create_table_sql += ',\nPRIMARY KEY(`%s`)' % primary_key_name
            create_table_sql += '\n) ENGINE=InnoDB DEFAULT CHARSET=utf8;'

            print("\nAbout to create table %s with this SQL statement:\n\n%s\n" % (table_name, create_table_sql) )

            # Execute CREATE TABLE statement.
            cursor.execute(create_table_sql)
            connection.commit()

            # Construct list of column names to be inserted.
            insert_columns_list = []
            for chosen_col_num in chosen_col_num_list:
                insert_columns_list.append(all_columns_list[chosen_col_num])
            insert_columns = "( " + ", ".join(insert_columns_list) + " )"

            # Construct bind variables expression.
            insert_values_bind = ["%s"]*len(insert_columns_list)
            insert_values_bind = "( " + ", ".join(insert_values_bind) + " )"

            # Construct the SQL insert.
            insert_sql = "INSERT INTO %s %s VALUES %s" % (table_name, insert_columns, insert_values_bind)

            print("You will be inserting records using this SQL:\n\n%s\n" % insert_sql)
        else:
            # The remaining lines of the file contain column values.
            # Construct list of values to be inserted.
            insert_values_list = []
            for chosen_col_num in chosen_col_num_list:
                insert_values_list.append(all_columns_list[chosen_col_num])
            insert_values_tuple = tuple(insert_values_list)

            # Do the insert using bind variables.
            cursor.execute(insert_sql, insert_values_tuple)
            if line_number % commit_frequency == 0:
                connection.commit()
                print("Inserted %d records." % commit_frequency)

# Commit any remaining inserts.
connection.commit()

# Finish up.
cursor.close()
connection.close()

print("All Done.")
