Summary: Command-line universal database client.

Version: 1.1

Author: David Joel Lambert

Date: January 22, 2018


Purpose:

A sample of the author's Python coding, to demonstrate that he can write decent 
Python, and that he knows relational databases.

Description:

This is a command-line program that asks an end-user for SQL to execute on one 
of five different relational databases: Microsoft SQL Server, PostgreSQL, MySQL, 
Microsoft Access, and SQLite. The code contains complete Oracle hooks, but no 
sample Oracle database exists (at the moment).  The author also has ambitions 
of adding code for DB2 and adding a DB2 sample database.

Bundled with this code are 3 VirtualBox Linux guests containing sample 
databases, one each for Microsoft SQL Server 2017 (which runs on several Linux 
distros!) PostgreSQL, and MySQL.  Sample Microsoft Access and SQLite databases 
are also included.  The sample databases all contain the same data: the small 
version of the Dell DVD Store database, version 2.1.

The code for the 5 implemented databases has been tested with CRUD statements
(Create, Read, Update, Delete).  There is nothing to prevent the end-user from
entering other SQL, such as ALTER DATABASE, CREATE VIEW, and BEGIN TRANSACTION,
but none have been tested.

A future version might include the ability to list databases, tables, views,
indexes, and their fields without having to know the structure of any data
dictionaries.  This is the easiest addition to make, so it is the most probable
addition to this package.
