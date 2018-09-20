""" testUniversalClientMain.py
Summary: Another Unit Test of universalClient.py.
Version: 0.1.0
Author: David Joel Lambert
Date: September 13, 2018

Purpose:  testUniversalClient.py does not test function main() in
universalClient.py as is, it skips all but one of the executions of input().

This module, testUniversalClientMain.py, tests main() as is, with all
executions of input().  It is not nearly as elegant, but it is much simpler
than testUniversalClient.py.
"""

from subprocess import run, PIPE
import sys

start = '''
Enter the number for your db type:
(1) oracle
(2) mysql
(3) ms sql server
(4) postgresql
(5) db2
(6) ms access
(7) sqlite, or
(Q) to Quit program: Selected sqlite.

Enter your db file's full path,
(Q) to Quit program, or
(S) to Start over: 
Enter SQL to execute in this db,
(Q) to Quit program, or
(A) to use Another db: '''

end = '''
Hit Enter to see rows 21-40,
(Q) to Quit program, or
(N) for No more rows: 
Quitting at your request.
'''

db_type = '7'
db_path = 'C:/Coding/PyCharm/projects/UniversalClient/databases/ds2.sqlite3'
sql_cmd = 'select * from customers;'
quit_cmd = 'Q'
commands = '{0}\n{1}\n{2}\n{3}\n'.format(db_type, db_path, sql_cmd, quit_cmd)
run_me = sys.executable + ' ../universalClient.py'
print(run_me)
result = run(run_me, input=commands, stdout=PIPE, stderr=PIPE, text=True)
assert result.returncode == 0
assert result.stderr == ''

file = open(file='expected_output.txt', mode='r')
query_output = file.read()
file.close()

assert result.stdout == start + query_output + end
