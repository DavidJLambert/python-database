""" testUniversalClient.py
Summary: Unit Test of universalClient.py.
Version: 0.1.0
Author: David Joel Lambert
Date: September 13, 2018
"""

from universalClient import *
import os.path
import sys
import inspect
import pytest


class TestClass(object):

    def test_do_stacktrace(self, capsys):
        try:
            raise ArithmeticError("Hello")
        except ArithmeticError:
            do_stacktrace()
            # Assemble expected output.
            file_path = os.path.abspath(__file__)
            except_name = sys.exc_info()[0].__name__
            except_str = str(sys.exc_info()[1])
            line_num = str(sys.exc_info()[2].tb_lineno)
        func_name = inspect.currentframe().f_code.co_name
        output = ('\nTraceback (most recent call last):\n  File "{0}", ' +
                  'line {1}, in {2}\n    raise {3}("{4}")\n{3}: {4}\n')
        variables = (file_path, line_num, func_name, except_name, except_str)
        expected = output.format(*variables)
        # Captured output.
        captured = capsys.readouterr()
        assert captured.out == expected

    def test_exception_user_quit(self):
        with pytest.raises(ExceptionUserQuit):
            raise ExceptionUserQuit('Hello')

    def test_exception_user_start_over(self):
        with pytest.raises(ExceptionUserStartOver):
            raise ExceptionUserStartOver('Hello')

    def test_exception_user_another_db(self):
        with pytest.raises(ExceptionUserAnotherDB):
            raise ExceptionUserAnotherDB('Hello')

    def test_exception_user_new_sql(self):
        with pytest.raises(ExceptionUserNewSQL):
            raise ExceptionUserNewSQL('Hello')

    def test_exception_fatal(self):
        with pytest.raises(ExceptionFatal):
            raise ExceptionFatal('Hello')

    def test_validate_file_path(self):
        validate_file_path('C:\Windows\System32')
        assert validate_file_path('C:\Windows\System32') is None
        with pytest.raises(FileNotFoundError):
            validate_file_path('Nonsense')

    def test_main(self, capsys, monkeypatch):
        """Would be great to simply call main() and feed series of commands
        through monkeypatch, but could not figure out how to do that."""
        with pytest.raises(ExceptionUserQuit):
            # Get default number of rows (20) from test database.
            connection = \
                connect_to_db('sqlite', '', 0, '', 'test.sqlite3', '', '')
            sql = 'select * from customers order by customerid asc;'
            cursor = run_sql(connection, sql)
            monkeypatch.setattr('builtins.input', lambda x: 'Q')
            print_response(cursor, sql)
        # Captured output.
        captured = capsys.readouterr()
        # Get expected output from file.
        file = open(file='expected_output.txt', mode='r')
        expected = file.read()
        file.close()
        # Compare captured to expected.
        assert captured.out == expected
