""" testUniversalClientUtils.py
REPOSITORY:
  https://github.com/DavidJLambert/Python-Universal-DB-Client

SUMMARY:
  For testUniversalClientSqlite and testUniversalClientMysql.

VERSION:
  0.2.3

AUTHOR:
  David J. Lambert

DATE:
  May 31, 2019

PURPOSE:
  testUniversalClient.py does not test function main() in
  universalClient.py as is, it skips all but one of the executions of input().

  This module tests main() as is for Mysql, with all executions of input().  It
  is not nearly as elegant, but it is much simpler than testUniversalClient.py.
"""

import sys
import os
from subprocess import run, PIPE

db_type_prompt = '''
Enter the number for your db type:
(1) oracle
(2) mysql
(3) sql server
(4) postgresql
(5) sqlite
(6) db2
(7) access, or
(Q) to Quit program: '''

sql_prompt = '''
Enter the SQL to execute in this db,
(Q) to Quit program, or
(A) to use Another db: '''

end_str = '''
Hit Enter to see more rows,
(Q) to Quit program, or
(N) for No more rows: 
Quitting at your request.
'''

file = open(file='expected_output.txt', mode='r')
query_output = file.read()
file.close()

py_file = 'universalClient.py'
if os.path.exists(py_file):
    run_me = sys.executable + ' ' + py_file
elif os.path.exists('../' + py_file):
    run_me = sys.executable + ' ../' + py_file
else:
    raise FileNotFoundError('Could not find {}.'.format(py_file))


def do_comparison(result, actual, expected):
    # Compare line by line, to make troubleshooting MUCH easier.
    actual = actual.splitlines()
    expected = expected.splitlines()
    for line in range(len(actual)):
        if actual[line] != expected[line]:
            print('Actual:   BEGIN{}END'.format(actual[line]))
            print('Expected: BEGIN{}END'.format(expected[line]))
        assert actual[line] == expected[line]

    if result.returncode != 0:
        print('result.returncode = {}'.format(result.returncode))
    assert result.returncode == 0

    if result.stderr:
        print('result.stderr = {}'.format(result.stderr))
    assert result.stderr == ''
    return
