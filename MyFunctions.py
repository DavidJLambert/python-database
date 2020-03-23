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


def skip_operation(sql: str) -> (bool, str):
    """ Detect whether to skip this operation and operations dependent on it
        Only used in DBClient.

    Parameters:
        sql (str): SQL from MyQueries.py.
    Returns:
        skip (bool): skip this operation and operations dependent on it.
    """
    from MyQueries import not_implemented, not_possible_sql
    skip = (sql in {not_implemented, not_possible_sql})
    return skip
# End of function print_stacktrace.


def os_python_version_info() -> (str, int, int):
    """ Method to print OS and Python version info.
        Only used in __main__.

    Parameters:
    Returns:
        os (str): 'Windows', 'Linux', or 'Darwin'
        Python major version (int): 2 or 3
        Python minor version (int)
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
    return u.system, sys_version_info[0], sys_version_info[1]
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


def file_in_path(os: str, filename: str) -> bool:
    """ Method to find if file in PATH.
        Only used in sql_cmdline.

    Parameters:
        os (str): 'Windows', 'Linux', or 'Darwin'
        filename (str): the name of the file to find in PATH.
    Returns:
        found (bool): whether or not the file was found in PATH.
    """
    from os import pathsep, environ, scandir

    filename = filename.lower()
    if os == 'Windows':
        filename += '.exe'

    found = False
    for folder in environ['PATH'].split(pathsep):
        if folder not in {'', '.'}:
            for f in scandir(folder):
                if f.is_file():
                    if f.name.lower() == filename:
                        found = True
                        break
    return found
# End of function file_in_path.


def sql_cmdline(os: str, sql: str, db_type: str, db_path: str, username: str,
                password: str, hostname: str, port_num: int, instance: str) -> list:
    """ Run SQL against database using command line client.
        Only used in __main__.

    Parameters:
        os (str): 'Windows', 'Linux', or 'Darwin'
        sql (str): text of the SQL to run.
        db_type (str): the database type.
        db_path (str): the location of the database file (Access and Sqlite).
        username (str): the username.
        password (str): the password for the user with username.
        hostname (str): the hostname of the database server.
        port_num (int): the port the server listens on.
        instance (str): the database instance.
    Returns:
        sql_output (list): rows of output.
    """
    from subprocess import Popen, PIPE

    db_client_exe = db_client_exes[db_type]
    if db_client_exe is None:
        z = '{} DOES NOT HAVE A COMMAND LINE INTERFACE.'
        print(z.format(db_type).upper())
        return list()

    if not file_in_path(os, db_client_exe):
        print('Did not find {} in PATH.'.format(db_client_exe))
        return list()

    cmd = ''
    if db_type == access:
        print('No command line available for MS Access.')
        return list()
    elif db_type == mysql:
        conn_str = '--uri={}:{}@{}:{}/{}'.format(username, password, hostname,
                                                 port_num, instance)
        cmd = [db_client_exe, conn_str, '--table', '--sql', '--quiet-start']
    elif db_type == oracle:
        conn_str = '{}/{}@{}:{}/{}'.format(username, password, hostname,
                                           port_num, instance)
        cmd = [db_client_exe, conn_str]
    elif db_type == postgresql:
        z = 'postgresql://{}:{}@{}:{}/{}'
        conn_str = z.format(username, password, hostname, port_num, instance)
        cmd = [db_client_exe, '-d', conn_str]
    elif db_type == sqlite:
        cmd = [db_client_exe, db_path]
    elif db_type == sqlserver:
        host_port = '{},{}'.format(hostname, port_num)
        cmd = [db_client_exe, '-U', username, '-P', password, '-S', host_port,
               '-d', instance, '-s', "|"]
    else:
        print('Not yet implemented for {}.'.format(db_type))
        return list()
    p = Popen(cmd,  stdin=PIPE, stdout=PIPE, stderr=PIPE)
    (stdout, stderr) = p.communicate(sql.encode('utf-8'))
    stderr = stderr.decode('utf-8').split("\n")
    # MySQL warning to ignore.
    z = [('WARNING: Using a password on the command line interface can be '
          'insecure.'), '']
    if len(stderr) > 0 and stderr != [''] and stderr != z:
        print("PROBLEM IN sql_cmdline:")
        print('cmd: ', cmd)
        print('sql: ', sql)
        print('stderr: ', stderr)
    return stdout.decode('utf-8').split("\n")
# End of function sql_cmdline.
