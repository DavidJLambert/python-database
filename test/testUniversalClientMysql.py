""" testUniversalClientMysql.py
https://github.com/DavidJLambert/Python-Universal-DB-Client

Summary: Another Unit Test of universalClient.py.  Using MySQL as representing
         enterprise databases on a server, with logins and instances.
Version: 0.2.1
Author: David J. Lambert
Date: October 9, 2018

Purpose:  testUniversalClient.py does not test function main() in
universalClient.py as is, it skips all but one of the executions of input().

This module, testUniversalClientMysql.py, tests main() as is for Mysql, with
all executions of input().  It is not nearly as elegant, but it is much simpler
than testUniversalClient.py.
"""

import testUniversalClientUtils as x

# Construct expected output from test of universalClient.

start_str = '''
## Selected mysql. ##

Enter the db server's host name or IP address,
(Q) to Quit program, or
(S) to Start over: 
Enter the port (the mysql default is 3306),
(Q) to Quit program, or
(S) to Start over: 
Enter the db instance,
(Q) to Quit program, or
(S) to Start over: 
Enter the db user name,
(Q) to Quit program, or
(S) to Start over: 
Enter root's password: '''

count_str = '''
## 20000 records were selected. ##
'''

expected_out = (x.db_type_prompt + start_str + x.sql_prompt + count_str +
                x.query_output + x.end_str)

# Find actual output from test of universalClient.

db_type = '2'
db_host = '192.168.1.65'
db_port = '3306'
db_instance = 'DS2'
db_user = 'root'
db_password = 'password'
sql_cmd = 'select * from CUSTOMERS;'
quit_cmd = 'Q'
cmds = ('{}\n'*8).format(db_type, db_host, db_port, db_instance, db_user,
                         db_password, sql_cmd, quit_cmd)
result = x.run(x.run_me, input=cmds, stdout=x.PIPE, stderr=x.PIPE, text=True)

actual_out = result.stdout

# Compare actual to expected.

x.do_comparison(result, actual_out, expected_out)
