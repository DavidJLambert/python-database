import pymysql

# query2 = query.format("%(actor)s", "%(price)s", terminator='')
# bind_vars2 = {'actor': 'CHEVY FOSTER', 'price': 35.0}

# MySQL database login and location information.
username = "root"
password = "password"
hostname = "127.0.0.1"
port_num = 3306
instance = "jingw"

# Print properties of the mysql.connector library.
print(f"\nmysql.connector library version: {pymysql.__version__}")
print(f"mysql.connector parameter style ('named', 'qmark', or 'pyformat'): {mysql.connector.paramstyle}")

# Make connection to database.
connection = pymysql.connect(user=username, password=password, host=hostname, port=port_num, database=instance)

# Create cursor.
cursor = connection.cursor()

sql = "delete from item where iid = 2"
cursor.execute(sql)

rows = cursor.fetchall()
print("Rows in result set:")
for row in rows:
    print(row)

# Commit any remaining inserts.
connection.commit()

# Finish up.
cursor.close()
connection.close()

print("All Done.")
