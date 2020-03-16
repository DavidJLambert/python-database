""" MyFunctions.py """

import sys
from MyConstants import *


def print_stacktrace() -> None:
    """ Print a stack trace, then resume.
        Used everywhere.

    Parameters:
    Returns:
    """
    from traceback import print_exception
    print()
    print_exception(*sys.exc_info(), limit=None, file=sys.stdout)
    return
# End of function print_stacktrace.


def no_none(might_be_none: str, if_its_none: str) -> str:
    """ If argument is None, change it.
        Only used in DBClient.

    Parameters:
        might_be_none (str): string that might be None.
        if_its_none (str): if might_be_none is None, change it to this.
    Returns:
        its_not_none (str): guaranteed to not be None.
    """
    if might_be_none is None:
        its_not_none = if_its_none
    else:
        its_not_none = might_be_none

    return its_not_none
# End of function no_none.


def os_python_version_info():
    """ Method to print OS and Python version info.
        Only used in __main__.

    Parameters:
    Returns:
    """
    from platform import uname, python_implementation
    from struct import calcsize

    q = ('OS: {}\nHost Name: {}\nOS Major Version: {}\nOS Full Version: {}'
         '\nProcessor Type: {}\nProcessor: {}')
    u = uname()
    q = q.format(u.system, u.node, u.release, u.version, u.machine, u.processor)
    print(q)

    sys_version_info = sys.version_info
    py_bits = 8 * calcsize("P")
    py_version = '.'.join(str(x) for x in sys_version_info)
    py_type = python_implementation()
    print('\n{} Version {}, {} bits.'.format(py_type, py_version, py_bits))
    return
# End of function ask_for_password.


def ask_for_password(username: str) -> str:
    """ Method to ask end-user for password for "username".
        Only used in __main__.

    Parameters:
        username (str): the username used to connect to this database instance.
    Returns:
        password (str): the password for "username".
    """
    from getpass import getpass
    prompt = "\nEnter {}'s password: ".format(username)  # Accept anything.
    if sys.stdin.isatty():
        # getpass works in terminal windows, but hangs in PyCharm (to fix, do
        # "Edit configurations" & select "Emulate terminal in output console").
        password = getpass(prompt)
    else:
        # In Eclipse & IDLE, getpass uses "input", which echoes password.
        password = input(prompt)
    return password
# End of function ask_for_password.


def run_sql_cmdline(sql: str, db_type: str) -> list:
    """ Run SQL against database using command line client.
        Only used in __main__.

    Parameters:
        sql (str): text of the SQL to run.
        db_type (str): the database type
    Returns:
        sql_output (list): rows of output.
    """
    from subprocess import Popen, PIPE
    if db_type == oracle:
        p = Popen(['sqlplus', '/nolog'],  stdin=PIPE, stdout=PIPE, stderr=PIPE)
        (stdout, stderr) = p.communicate(sql.encode('utf-8'))
        return stdout.decode('utf-8').split("\n")
    elif db_type == sql_server:
        pass
        # sqlcmd -S 192.168.1.64,1433 -d ds2 -U sa -P password
        # p = Popen(['sqlcmd', '-S', hostname,port_num, '-d', instance, '-U', username, '-P', password],  stdin=PIPE, stdout=PIPE, stderr=PIPE)
        # (stdout, stderr) = p.communicate(sql.encode('utf-8'))
        # return stdout.decode('utf-8').split("\n")
    elif db_type == sqlite:
        p = Popen(['C:/Program Files/SQLite/tools/sqlite3.exe'],  stdin=PIPE, stdout=PIPE, stderr=PIPE)
        (stdout, stderr) = p.communicate(sql.encode('utf-8'))
        return stdout.decode('utf-8').split("\n")
    elif db_type == access:
        return list()
    else:
        print('Not yet implemented for {}.'.format(db_type))
# End of function run_sql_cmdline.
