""" testUniversalClientSqlite.py
Summary: Another Unit Test of universalClient.py.
Version: 0.1.0
Author: David Joel Lambert
Date: September 21, 2018

Purpose:  testUniversalClient.py does not test function main() in
universalClient.py as is, it skips all but one of the executions of input().

This module, testUniversalClientSqlite.py, tests main() as is for SQLite, with
all executions of input().  It is not nearly as elegant, but it is much sx.er
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

db_type = '7'
db_path = r'C:\Coding\PyCharm\projects\Python-Universal-DB-Client\test\test.sqlite3'
sql_cmd = 'select * from CUSTOMERS;'
quit_cmd = 'Q'
cmds = '%s\n'*4 % (db_type, db_path, sql_cmd, quit_cmd)

result = x.run(x.run_me, input=cmds, stdout=x.PIPE, stderr=x.PIPE, text=True)

actual_out = result.stdout

# Compare actual to expected.

x.do_comparison(result, actual_out, expected_out)
