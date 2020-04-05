""" MyFunctions.py

REPOSITORY: https://github.com/DavidJLambert/Python-Universal-DB-Client

AUTHOR: David J. Lambert

VERSION: 0.7.3

DATE: Apr 5, 2020
"""
import sys


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


def is_skip_operation(sql: str) -> bool:
    """ Detect whether to skip this operation and operations dependent on it
        Only used in DBClient.py.

    Parameters:
        sql (str): SQL from MyQueries.py.
    Returns:
        skip (bool): skip this operation and operations dependent on it.
    """
    from MyQueries import not_implemented, not_possible_sql
    skip = (sql in {not_implemented, not_possible_sql})
    return skip
# End of function is_skip_operation.


def os_python_version_info() -> (str, int, int):
    """ Method to print OS and Python version info.
        Only used in UniversalClient*.py.

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
# End of function os_python_version_info.


def is_file_in_path(os: str, filename: str) -> bool:
    """ Method to find if file in PATH.
        Only used in DBInstance.py.

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
# End of function is_file_in_path.


def pick_one(choices: list, choice_type: str) -> int:
    """ Function to print message if skipping operation in
        db_table_schema or db_view_schema.
        Only used in DBClient.py.

    Parameters:
        choices (list): list of choices (string) to pick one from.
        choice_type (str): the name of the type being chosen.
    Returns:
        choice (int): the choice made, or -1 if no choices are available.
    """
    if len(choices) == 0:
        choice = -1
        print('You own no {}s!  Nothing to see!'.format(choice_type))
    elif len(choices) == 1:
        # Only one table.  Choose it.
        choice = 0
        choice_name = choices[choice]
        print('\nYou have one {}: {}.'.format(choice_type, choice_name))
    else:
        # Ask end-user to choose a table.
        prompt = '\nHere are the {} available to you:\n'.format(choice_type)
        for item_num, choice_name in enumerate(choices):
            prompt += str(item_num + 1) + ': ' + choice_name + '\n'
        prompt += ('Enter the number for the {} you want info about,\n'
                   'Or enter "Q" to quit:\n'.format(choice_type))

        # Keep looping until valid choice made.
        while True:
            choice = input(prompt).strip().upper()
            # Interpret the choice.
            if choice == "Q":
                print('Quitting as requested.')
                # Not cleaning up connections and cursors.
                exit(0)
            choice = int(choice) - 1
            if choice in range(len(choices)):
                choice_name = choices[choice]
                break
            else:
                print('Invalid choice, please try again.')
        print('\nYou chose {} "{}":.'.format(choice_type, choice_name))

    return choice
# End of function pick_one.


def sql_cmdline(cmdline_list: list, sql: str) -> list:
    """ Run SQL against database using command line client.

    Parameters:
        cmdline_list (list): the list of commands for Popen.
        sql (str): text of the SQL to run.
    Returns:
        sql_output (list): rows of output.
    """
    if cmdline_list[0] == 'Error':
        print(cmdline_list[1])
        return list()

    from subprocess import Popen, PIPE
    p = Popen(cmdline_list,  stdin=PIPE, stdout=PIPE, stderr=PIPE)
    (stdout, stderr) = p.communicate(sql.encode('utf-8'))
    stderr = stderr.decode('utf-8').split("\n")
    # MySQL warning to ignore.
    z = [('WARNING: Using a password on the command line interface can be '
          'insecure.'), '']
    if len(stderr) > 0 and stderr != [''] and stderr != z:
        print("PROBLEM IN SQL_CMDLINE:")
        print('cmd: ', cmdline_list)
        print('sql: ', sql)
        print('stderr: ', stderr)

    return stdout.decode('utf-8').split("\n")
# End of function sql_cmdline.


def quote_a_string(quote_me: str) -> str:
    """ Enclose a string in single quotes, after escaping slashes and single
        quotes.

    Parameters:
        quote_me (str): the string to quote.
    Returns:
        output (list): the string enclosed in single quotes.
    """
    quote_me2 = quote_me.replace("'", "''")
    return "'" + quote_me2 + "'"
# End of function quote_a_string.
