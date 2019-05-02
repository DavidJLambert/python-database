Universal Database Client
-------------------------

SUMMARY:
  - Command-line universal database client.
  - https://github.com/DavidJLambert/Python-Universal-DB-Client

VERSION:
  0.2.1

AUTHOR:
  David J. Lambert

DATE:
  October 9, 2018

PURPOSE:
  A sample of my Python coding, to demonstrate that I can write decent Python,
  test it, and document it.  I also demonstrate that I know relational
  databases and Linux.

DESCRIPTION:
  This is a command-line program that asks an end-user for SQL to execute on 1
  of 7 different relational databases, ordered by popularity as ranked in 
  https://pypl.github.io/DB.html in Oct 2018:

  - Oracle
  - MySQL
  - Microsoft SQL Server
  - PostgreSQL
  - SQLite
  - IBM DB2 (untested)
  - Microsoft Access 2016

  I also provide sample databases to run this program against (see below).
  The code for DB2 is untested.

  The code for the 6 tested database types has been tested with CRUD statements
  (Create, Read, Update, Delete).  There is nothing to prevent the end-user
  from entering other SQL, such as ALTER DATABASE, CREATE VIEW, and BEGIN
  TRANSACTION, but none have been tested.

  A future version might include the ability to list databases, tables, views,
  indexes, and their fields without having to know the structure of any data
  dictionaries.  This is the easiest addition to make, so it is the most
  probable addition to this package.

PROGRAM REQUIREMENTS:

  + For connecting to Oracle, my code uses the cx_Oracle library, which is
    available on PyPI.  The cx_Oracle library requires the Oracle client
    libraries.  Several ways to obtain the Oracle client libraries are
    documented on https://cx-oracle.readthedocs.io/en/latest/installation.html.

    Cx_Oracle v7.0.0 supports Python versions 2.7 and 3.5-3.7, and Oracle
    versions 11.2-18.3.

  + For connecting to MySQL, my code uses the pymysql library, which is
    available on PyPI.

    Pymysql v0.9.2 supports Python versions 2.7 and 3.4-3.7, plus MySQL and
    MariaDB versions 5.5 and newer.

  + For connecting to Microsoft SQL Server, my code uses the pymssql library,
    which is available on PyPI.  The pymssql library requires Microsoft Visual
    C++ 14, which is available as "Microsoft Visual C++ Build Tools" on
    http://landinghub.visualstudio.com/visual-cpp-build-tools.

    Pymssql v2.1.4 supports Python versions 2.7 and 3.4-3.7, and Microsoft SQL
    Server versions 2005 and newer.

  + For connecting to PostgreSQL, my code uses the psycopg2 library, which
    is available on PyPI.

    Psycopg2 v2.7.5 supports Python versions 2.6-2.7 and 3.2-3.6, and
    PostgreSQL server versions 7.4-10.  Python 3.7 is unsupported, but I had no
    problems using it.

  + For connecting to IBM DB2, my code uses the ibm_db library, which is
    available on PyPI.  The ibm_db library library requires Microsoft Visual
    C++ 14, which is available as "Microsoft Visual C++ Build Tools" on
    http://landinghub.visualstudio.com/visual-cpp-build-tools.

    I can not find which Python versions are supported by ibm_db v2.0.9.  I had
    no problems installing it in Python versions 2.7 and 3.3-3.7.

  + For connecting to Microsoft Access 2016, my code uses the pyodbc library,
    which is available on PyPI.  The pyodbc library requires the "Microsoft
    Access Database Engine 2016 Redistributable", which is available from
    https://www.microsoft.com/en-us/download/details.aspx?id=54920.

    Pyodbc v4.0.24 supports Python versions 2.7 and 3.4-3.6.

  + For connecting to SQLite, my code uses the sqlite3 library, part of the
    Python Standard Library.

    The sqlite3 library has been in the Standard Library since Python 2.5.

SAMPLE DATABASES TO TEST THIS PROGRAM ON:
  I provide 5 sample databases to run this program against, one for each of the
  5 types of tested database types listed in the previous section.  I have
  ambitions of creating sample Oracle and DB2 databases.

  Sample SQLite and Microsoft Access databases are included in this package in
  these locations:

  - databases/ds2.sqlite3
  - databases/ds2.accdb

  There are 3 VirtualBox Linux guests containing sample databases, one each for
  Microsoft SQL Server on Ubuntu (officially supported!), MySQL on Debian, and
  PostgreSQL on Debian.

  - MySQL:                https://1drv.ms/u/s!AieKzIY33GmRgcExQPbjBZ62X4tPCQ
  - Microsoft SQL Server: https://1drv.ms/u/s!AieKzIY33GmRgcQXIZ9mvqPNcEqHdw
  - PostgreSQL:           https://1drv.ms/u/s!AieKzIY33GmRgcEwOQinckQ9Buyk9w

  The sample databases all have the same data: the small version of the Dell
  DVD Store database, version 2.1, available at http://linux.dell.com/dvdstore.
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
    - Available at https://1drv.ms/u/s!AieKzIY33GmRgcExQPbjBZ62X4tPCQ.
    - MySQL 5.5.60 on an Oracle VirtualBox virtual machine running Debian 8.11
      Jessie.  I've installed LXDE desktop 0.99.0-1 on it.
    - This virtual machine is based on a virtual machine created by Turnkey
      Linux (Turnkey GNU/Linux version 14.2), which is available at
      https://www.turnkeylinux.org/mysql.

  The Microsoft SQL Server sample database:
    - Available at https://1drv.ms/u/s!AieKzIY33GmRgcQXIZ9mvqPNcEqHdw.
    - Microsoft SQL Server 2017 Express Edition on an Oracle VirtualBox virtual
      machine running Ubuntu 16.04.3 server.  No desktop environment, command
      line only.
    - This virtual machine was installed from a Ubuntu 16.04.3 server iso image
      downloaded from https://www.ubuntu.com/download/server.

  The PostgreSQL sample database:
    - Available at https://1drv.ms/u/s!AieKzIY33GmRgcEwOQinckQ9Buyk9w.
    - PostgreSQL 9.4.19 on an Oracle VirtualBox virtual machine running Debian
      8.11 Jessie.  I've installed LXDE desktop 0.99.0-1 on it.
    - This virtual machine is based on a virtual machine created by Turnkey
      Linux (Turnkey GNU/Linux version 14.2), which is available at
      https://www.turnkeylinux.org/mysql.

  The Microsoft Access 2016 sample database:
    - Included in this package as databases/ds2.accdb.

  The SQLite sample database:
    - Included in this package as databases/ds2.sqlite3.
