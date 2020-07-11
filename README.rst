=========================
Universal Database Client
=========================

:SUMMARY: Command-line universal database client.

:REPOSITORY: https://github.com/DavidJLambert/Python-Universal-DB-Client

:AUTHOR: David J. Lambert

:VERSION: 0.7.7

:DATE: Jul 10, 2020

PURPOSE
-------
A sample of my Python coding, to demonstrate that I can write decent Python,
test it, and document it.  I also demonstrate I know relational databases.

DESCRIPTION
-----------
This is a command-line program that executes SQL to execute on 1 of 6 types of
relational databases:

- Access
- MySQL
- Oracle
- PostgreSQL
- SQLite
- SQL Server

I also provide sample databases to run this program against (see below).

The code for the 6 tested database types has been tested with CRUD statements
(Create, Read, Update, Delete).  Other SQL, such as ALTER DATABASE, CREATE VIEW,
and BEGIN TRANSACTION have not been tested.

Class DBInstance encapsulates all the info needed to log into a database
instance, plus it encapsulates the connection handle to that database.  Its
externally useful methods are:

1.  print_all_connection_parameters: prints all the connection parameters.
2.  close_connection: closes the connect to the database.
3.  get_connection_status: whether or not DBInstance is connected to the db.
4.  sql_cmdline: runs the db command line client (sqlplus, sqlcmd, etc.) as a
    subprocess.

Class DBClient executes SQL with bind variables, and then prints the results.
Its externally useful methods are:

1.  set_sql: gets the text of SQL to run.
2.  set_bind_vars: gets the bind variables for sql.
3.  run_sql: executes SQL, which was read with set_sql and set_bind_vars.
4.  db_table_schema: lists all the tables owned by the current login,
    all the columns in those tables, and all indexes on those tables.
5.  db_view_schema: lists all the views owned by the current login, all
    the columns in those views, and the SQL for the view.

Class OutputWriter handles all query output to file or to standard output.
Its externally useful methods are:

1.  get_align_col: whether or not to align columns in output.
2.  get_col_sep: get the character(s) to separate columns with.
3.  get_out_file_name: get location to write output to (file or standard out).
4.  write_rows: write output to location chosen in get_out_file_name.
5.  close_output_file: if writing to output file, close it.

The code has been tested with CRUD statements (Create, Read, Update, Delete).
There is nothing to prevent the end-user from entering other SQL, such as
ALTER DATABASE, CREATE VIEW, and BEGIN TRANSACTION, but none have been tested.

This program loads the entire result set into memory.  Thus, it is unsuitable
for large results sets, which may not fit in the host's available RAM.

PROGRAM REQUIREMENTS
--------------------
+ For connecting to Oracle, my code uses the cx_Oracle library, which is
  available on PyPI.  The cx_Oracle library requires the Oracle client
  libraries.  Several ways to obtain the Oracle client libraries are documented
  on https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html.

  Cx_Oracle v7.3.0 supports Python versions 2.7 and 3.5-3.8, and Oracle client
  versions 11.2-19.

+ For connecting to MySQL, my code uses the pymysql library, which is available
  on PyPI.

  Pymysql v0.9.3 supports Python versions 2.7 and 3.4-3.8, plus MySQL and
  MariaDB versions 5.5 and newer.

+ For connecting to Microsoft SQL Server, my code originally used the pymssql
  library, which was available on PyPI, but the pymssql project has been
  discontinued.  Instead, I use pyodbc.  Turbodbc has a reputation for being
  faster than pyodbc, but I got fatal errors when trying to install it.

+ For connecting to PostgreSQL, my code uses the psycopg2 library, which is
  available on PyPI.

  Psycopg2 v2.8.4 supports Python version 2.7 and 3.4-3.8, and PostgreSQL server
  versions 7.4-12.

+ For connecting to Microsoft Access and Microsoft SQL Server, my code uses the
  pyodbc library, which is available on PyPI.  The pyodbc library requires the
  "Microsoft Access Database Engine 2016 Redistributable", which is available
  from https://www.microsoft.com/en-us/download/details.aspx?id=54920.

  Pyodbc v4.0.30 supports Python versions 2.7 and 3.4-3.8.

+ For connecting to SQLite, my code uses the sqlite3 library, part of the Python
  Standard Library.

  The sqlite3 library has been in the Standard Library since Python 2.5.

SAMPLE DATABASES TO TEST THIS PROGRAM ON
----------------------------------------
I provide 5 sample databases to run this program against, one for each of the
five types of tested database types listed in the previous section.  I have a
test Oracle database on Windows, which is obviously not suitable for a freely
downloadable sample database.

Sample SQLite and Microsoft Access databases are included in this package in
these locations:

- databases/ds2.sqlite3
- databases/ds2.accdb

There are 3 VirtualBox Linux guests containing sample databases, one each for
Microsoft SQL Server on Ubuntu (officially supported!), MySQL on Debian, and
PostgreSQL on Debian.

- Microsoft SQL Server:
  https://1drv.ms/u/s!AieKzIY33GmRgd9GYSfUxOHbOlpoyw?e=wpozlv
- MySQL:
  https://1drv.ms/u/s!AieKzIY33GmRgc1zW_xlX5Eeyqztug?e=YYeaTg
- PostgreSQL:
  https://1drv.ms/u/s!AieKzIY33GmRgeAGzgQcIm-6gBO7CQ?e=hv7mnS

The sample databases all have the same data: the small version of the Dell DVD
Store database, version 2.1, available at http://linux.dell.com/dvdstore.
The data is in these tables:

- CATEGORIES     --     16 records
- CUSTOMERS      -- 20,000 records
- CUST_HIST      -- 60,350 records
- INVENTORY      -- 10,000 records
- ORDERLINES     -- 60,350 records
- ORDERS         -- 12,000 records
- PRODUCTS       -- 10,000 records
- REORDER        --      0 records
- I've added table db_description, containing 1 record with my name and
  contact information.

The MySQL sample database:

- Available at https://1drv.ms/u/s!AieKzIY33GmRgc1zW_xlX5Eeyqztug?e=YYeaTg.
- MySQL 5.5.60 on an Oracle VirtualBox virtual machine running Debian 8.11
  Jessie.  I've installed LXDE desktop 0.99.0-1 on it.
- This virtual machine is based on a virtual machine created by Turnkey Linux
  (Turnkey GNU/Linux version 14.2), available at
  https://www.turnkeylinux.org/mysql.

The Microsoft SQL Server sample database:

- Available at https://1drv.ms/u/s!AieKzIY33GmRgd9GYSfUxOHbOlpoyw?e=wpozlv.
- Microsoft SQL Server 2017 Express Edition on an Oracle VirtualBox virtual
  machine running Ubuntu 16.04.3 server, with desktop, command line only.
- This virtual machine was installed from a Ubuntu 16.04.3 server iso image
  downloaded from https://www.ubuntu.com/download/server.

The PostgreSQL sample database:

- Available at https://1drv.ms/u/s!AieKzIY33GmRgeAGzgQcIm-6gBO7CQ?e=hv7mnS.
- PostgreSQL 12.2.0-1 on an Oracle VirtualBox virtual machine running Debian
  9.12 Stretch, with LXDE desktop.
- This virtual machine is based on a virtual machine created by Bitnami, which
  was downloaded from https://bitnami.com/stack/postgresql/virtual-machine.
  Documentation for that virtual machine can be found at
  https://docs.bitnami.com/virtual-machine/infrastructure/postgresql.

The Microsoft Access 2016 sample database:

- Included in this package as databases/ds2.accdb.

The SQLite sample database:

- Included in this package as databases/ds2.sqlite3.
