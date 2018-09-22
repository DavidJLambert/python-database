import sys
import os
from subprocess import run, PIPE

db_type_prompt = '''
Enter the number for your db type:
(1) oracle
(2) mysql
(3) sql server
(4) postgresql
(5) db2
(6) access
(7) sqlite, or
(Q) to Quit program: '''

sql_prompt = '''
Enter the SQL to execute in this db,
(Q) to Quit program, or
(A) to use Another db: '''

end_str = '''
Hit Enter to see rows 21-40,
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
    raise FileNotFoundError('Could not find %s.' % py_file)


def do_comparison(result, actual, expected):
    if result.returncode != 0:
        print('result.returncode = %s' % str(result.returncode))
    assert result.returncode == 0
    if result.stderr:
        print('result.stderr = %s' % str(result.stderr))
    assert result.stderr == ''

    # Compare line by line, to make troubleshooting MUCH easier.
    actual = actual.splitlines()
    expected = expected.splitlines()

    for line in range(len(actual)):
        if actual[line] != expected[line]:
            print('Actual:   BEGIN%sEND' % actual[line])
            print('Expected: BEGIN%sEND' % expected[line])
        assert actual[line] == expected[line]
    return
