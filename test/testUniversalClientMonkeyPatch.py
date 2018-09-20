# EXPERIMENTAL, DO NOT USE

from universalClient import *
import os.path
import sys
import inspect
import pytest

# -------- DEFINE TESTS

# input_text = ('g\n', 'test/test.sqlite3\n', 'select * from customers;\n', '\n', 'n\n', 'q\n')


class TestClass(object):

    def test_do_stacktrace(self, capsys):
        try:
            raise ArithmeticError("Hello")
        except ArithmeticError:
            do_stacktrace()
            file_path = os.path.abspath(__file__)
            except_name = sys.exc_info()[0].__name__
            except_str = str(sys.exc_info()[1])
            line_num = str(sys.exc_info()[2].tb_lineno)
            func_name = inspect.currentframe().f_code.co_name
            expect = ('\nTraceback (most recent call last):\n' +
                      '  File "' + file_path + '", line ' + line_num +
                      ', in ' + func_name + '\n' +
                      '    raise ' + except_name + '("' + except_str + '")\n' +
                      except_name + ': ' + except_str + '\n')
        captured = capsys.readouterr()
        assert captured.out == expect

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

    # Everything above is complete.

    def test_main(self, capsys, monkeypatch):
        input_text = ('g\n' +
                      'test/test.sqlite3\n' +
                      'select * from customers;\n' +
                      '\n' +
                      'n\n' +
                      'q\n')
        # input_text = 'q\n'
        # file = open(file='expected_output.txt', mode='r')
        # expect = file.read()
        # file.close()
        expect = "What?"
        monkeypatch.setattr('builtins.input', lambda x: input_text)
        main()
        captured = capsys.readouterr()
        assert captured.out == expect

    """

    # Use test.sqlite3
    ask_for_db_type()
    ask_for_db_location(db_type)
    ask_for_db_instance(db_type)
    ask_for_db_login(db_type)
    connect_to_db(db_type, db_host, db_port, db_instance, db_path, db_user, db_password)
    ask_for_sql()
    run_sql(connection, sql)
    print_response(cursor, sql)
    disconnect_db(db_type, connection, cursor)

    def test_connect_to_db(db_type, db_host, db_port, db_instance, db_path, db_user, db_password):
        connect_to_db(db_type, db_host, db_port, db_instance, db_path, db_user, db_password)


    def test_run_sql(connection, sql):
        run_sql(connection, sql)


    def test_print_response(cursor, sql):
        print_response(cursor, sql)


    def test_disconnect_db(db_type, connection, cursor):
        disconnect_db(db_type, connection, cursor)

    # Asks.  Do we really need to test these?

    def test_ask_for_db_type(self):
        import mock
        with mock.patch.object(__builtin__, 'input', lambda: 'input_text'):
            assert module.function() == 'expected_output'
        ask_for_db_type() #is an invalid choice


    def test_ask_for_db_location(db_type):
        ask_for_db_location(db_type)


    def test_ask_for_db_instance(db_type):
        ask_for_db_instance(db_type)


    def test_ask_for_db_login(db_type):
        ask_for_db_login(db_type)


    def test_ask_for_sql():
        ask_for_sql()


    def test_ask_end_user(prompt):
        ask_end_user(prompt)
    """
