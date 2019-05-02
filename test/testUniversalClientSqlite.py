""" testUniversalClientSqlite.py
https://github.com/DavidJLambert/Python-Universal-DB-Client

Summary: Another Unit Test of universalClient.py.  Using SQLite as representing
         simple databases on the local file system, with no login.
Version: 0.2.1
Author: David J. Lambert
Date: October 9, 2018

Purpose:  testUniversalClient.py does not test function main() in
universalClient.py as is, it skips all but one of the executions of input().

This module, testUniversalClientSqlite.py, tests main() as is for SQLite, with
all executions of input().  It is not nearly as elegant, but it is much simpler
than testUniversalClient.py.
"""

import testUniversalClientUtils as x

# Construct expected output from test of universalClient.

start_str = '''
## Selected sqlite. ##

Enter your db file's full path,
(Q) to Quit program, or
(S) to Start over: '''

expected_out = (x.db_type_prompt + start_str + x.sql_prompt +
                x.query_output + x.end_str)

# Find actual output from test of universalClient.

db_type = '5'
db_path = r'C:\Coding\PyCharm\projects\Python-Universal-DB-Client\test\test.sqlite3'
sql_cmd = 'select * from CUSTOMERS;'
quit_cmd = 'Q'
cmds = ('{}\n'*4).format(db_type, db_path, sql_cmd, quit_cmd)

result = x.run(x.run_me, input=cmds, stdout=x.PIPE, stderr=x.PIPE, text=True)

actual_out = result.stdout

# Compare actual to expected.

x.do_comparison(result, actual_out, expected_out)
