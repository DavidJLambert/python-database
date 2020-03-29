""" DBInstance.py """
from MyConstants import *
from MyFunctions import *


class DBInstance(object):
    """ Class containing database connection utilities and information
        about one database instance (referred to as "this database").

    Attributes:
        db_type (str): the type of database (Oracle, SQL Server, etc).
        db_path (str): the path of the database file, for SQLite and Access.
        username (str): a username for connecting to this database.
        password (str): the password for "username".
        hostname (str): the hostname of this database.
        port_num (int): the port this database listens on.
        instance (str): the name of this database instance.
        db_lib_obj (object): imported db library object.
        db_lib_name (str): name of the imported db library.
        db_lib_version (str): version of the imported db library.
        db_software_version (str): version of the database software being used.
        connection: the handle to this database. I set connection = None when
                    connection closed, this is not default behavior.
        callers (dict): collection of DBClient instances using this instance of
                        of DBInstance, along with their cursor objects.
    """
    def __init__(self, db_type: str, db_path: str, username: str, password: str,
                 hostname: str, port_num: int, instance: str) -> None:
        """ Constructor method for this class.

        Parameters:
            db_type (str): the type of database (Oracle, SQL Server, etc).
            db_path (str): the path of the database file, for SQLite and Access.
            username (str): the username for the connection to this database.
            password (str): the password for "username".
            hostname (str): the hostname of this database.
            port_num (int): the port this database listens on.
            instance (str): the name of this database instance.
        Returns:
        """
        # Save arguments of __init__.
        self.db_type: str = db_type
        self.db_path: str = db_path
        self.username: str = username
        self.password: str = password
        self.hostname: str = hostname
        self.port_num: int = port_num
        self.instance: str = instance

        # Check if db_type valid.
        if self.db_type not in db_types:
            print('Invalid database type "{}".'.format(self.db_type))
            exit(1)

        # Import appropriate database library.
        self.db_lib_name = lib_name_for_db[self.db_type]
        self.db_lib_obj = __import__(self.db_lib_name)

        # Appropriate database client executable.
        self.db_client_exe = db_client_exes[self.db_type]

        # Get database library version.
        if self.db_lib_name in {psycopg2, pymysql}:
            self.db_lib_version = self.db_lib_obj.__version__
        else:
            self.db_lib_version = self.db_lib_obj.version
        z = 'Using {} library version {}.'
        print(z.format(self.db_lib_name, self.db_lib_version))

        # Get the library's primary parameter style.
        print('Parameter style "{}".'.format(self.db_lib_obj.paramstyle))
        # paramstyle = 'named': cx_Oracle.  Option for sqlite3 & psycopg2.
        # paramstyle = 'qmark': sqlite3 and pyodbc.
        # paramstyle = 'pyformat': pymysql and psycopg2.

        # Get the parameter style we're using.
        self.paramstyle = paramstyle_for_lib[self.db_lib_name]
        if self.paramstyle in {named, pyformat}:
            self.bind_vars = dict()
        elif self.paramstyle == qmark:
            self.bind_vars = tuple()

        # Connect to database instance.
        self.connection = None
        try:
            if db_type in uses_connection_string:
                z = self.get_db_connection_string()
                self.connection = self.db_lib_obj.connect(z)
            else:
                args = (self.hostname, self.username, self.password,
                        self.instance, self.port_num)
                self.connection = self.db_lib_obj.connect(*args)
            print('Successfully connected to database.')
        except self.db_lib_obj.Error:
            print_stacktrace()
            print('Failed to connect to database.')
            exit(1)

        # Get database software version.
        self.db_software_version = self.get_db_software_version()

        # Prepare to save cursors for this connection.
        self.callers = dict()

        return
    # End of method __init__.

    # METHODS INVOLVING THE DATABASE CONNECTION.

    def close_connection(self, del_cursors: bool = False) -> None:
        """ Method to close connection to this database.

        Parameters:
            del_cursors (bool): if True, first delete dependent cursors,
                                if False, refuse to close connection.
        Returns:
        """
        if del_cursors:
            # TODO need to test with multiple DBClients.
            for caller, cursor in self.callers.items():
                self.delete_cursor(caller)
                del caller
        else:
            print('Dependent DBClients exist, will not close connection.')

        if self.db_type in file_databases:
            z = '\n{} from database at "{}".'
            z = z.format('{}', self.db_path)
        else:
            z = '\n{} from instance "{}" on host "{}".'
            z = z.format('{}', self.instance, self.hostname)
        try:
            self.connection.close()
            self.connection = None
            print(z.format('Successfully disconnected'))
        except self.db_lib_obj.Error:
            print_stacktrace()
            print(z.format('Failed to disconnect'))
            exit(1)
        return
    # End of method close_connection.

    def create_cursor(self, caller):
        """ Method that creates and returns a new cursor.  Saves caller
            object, along with its cursor, into "callers", so that deletion of
            self cannot be done before dependent callers and their cursors are
            deleted.

        Parameters:
            caller: the self object of the object calling create_cursor, added
                    to pool of caller objects.
        Returns:
            cursor: handle to this database.
        """
        cursor = self.connection.cursor()
        self.callers[caller] = cursor
        return cursor
    # End of method create_cursor.

    def delete_cursor(self, caller):
        """ Method that deletes an existing cursor.

        Parameters:
            caller: the DBClient object calling delete_cursor.
                    Remove from pool of caller objects.
        Returns:
        """
        # Close cursor.
        self.callers[caller].close()
        # Delete caller DBClient from callers pool.
        del self.callers[caller]
        return
    # End of method delete_cursor.

    # DATABASE INFORMATION METHODS.

    def get_connection_status(self) -> str:
        """ Method that prints whether or not connected to this database.

        Parameters:
        Returns:
        """
        if self.db_type in file_databases:
            z = 'Connection status for the database at "{}": {}connected.'
            z = z.format(self.db_path, '{}')
        else:
            z = 'Connection status for instance "{}", host "{}": {}connected.'
            z = z.format(self.instance, self.hostname, '{}')
        if self.connection is not None:
            z = z.format('')
        else:
            z = z.format('not ')
        return z
    # End of method get_connection_status.

    def get_db_lib_name(self) -> str:
        """ Method to return the name of the needed database library.

        Parameters:
        Returns:
            db_lib_name (str): database library name.
        """
        return self.db_lib_name
    # End of method get_db_lib_name.

    def get_db_type(self) -> str:
        """ Method to return the database type.

        Parameters:
        Returns:
            db_type (str): database software type.
        """
        return self.db_type
    # End of method get_db_type.

    def init_bind_vars(self):
        """ Method to return an empty data structure for bind variables.

        Parameters:
        Returns:
            bind_vars: empty data structure for bind variables.
        """
        return self.bind_vars
    # End of method init_bind_vars.

    def get_paramstyle(self):
        """ Method to return the parameter style.

        Parameters:
        Returns:
            paramstyle (str): the parameter style.
        """
        return self.paramstyle
    # End of method get_paramstyle.

    def get_db_software_version(self) -> str:
        """ Method to return the database software version.

        Parameters:
        Returns:
            db_software_version (str): database software version.
        """
        sql = 'Nada'
        if self.db_type in {mysql, postgresql}:
            sql = 'SELECT version()'
        elif self.db_type == oracle:
            sql = "SELECT * FROM v$version WHERE banner LIKE 'Oracle%'"
        elif self.db_type == sqlite:
            sql = 'select sqlite_version()'
        elif self.db_type == sqlserver:
            sql = 'SELECT @@VERSION'

        if sql != 'Nada':
            cursor = self.connection.cursor()
            cursor.execute(sql)
            version = cursor.fetchone()[0]
            cursor.close()
            del cursor
        elif self.db_type == access:
            version = "unavailable for MS Access through SQL"
        elif self.db_lib_name != pyodbc:
            version = self.connection.version
        else:
            version = 'unknown'
        return str(version)
    # End of method get_db_software_version.

    def get_db_connection_string(self):
        """ Method to form database connection string.

        Parameters:
        Returns:
            db_software_version (str): database software version.
        """
        z = ''
        if self.db_lib_name == cx_Oracle:
            z = '{}/{}@{}:{}/{}'
        elif self.db_lib_name == psycopg2:
            z = "user='{}' password='{}' host='{}' port='{}' dbname='{}'"
        elif self.db_lib_name == pymysql:
            z = ''
        elif self.db_lib_name == pyodbc:
            if self.db_type == access:
                z = ('DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};'
                     'DBQ={};')
            elif self.db_type == sqlserver:
                z = ('DRIVER={{SQL Server}};UID={};PWD={};SERVER={};PORT={};'
                     'DATABASE={}')
        elif self.db_lib_name == sqlite3:
            z = '{}'
        else:
            print('Unknown db library "{}", aborting.'.format(self.db_lib_name))
            exit(1)

        if self.db_type in {oracle, postgresql, sqlserver}:
            z = z.format(self.username, self.password, self.hostname,
                         self.port_num, self.instance)
        elif self.db_type in file_databases:
            z = z.format(self.db_path)
        return z
    # End of method get_db_software_version.

    def print_all_connection_parameters(self) -> None:
        """ Method that executes all print methods of this class.

        Parameters:
        Returns:
        """
        print('The database type is "{}".'.format(self.db_type))
        z = 'The database software version is {}.'
        print(z.format(self.db_software_version))
        if self.db_type in file_databases:
            print('The database path is "{}".'.format(self.db_path))
        else:
            print('The database username is "{}".'.format(self.username))
            print('The database hostname is "{}".'.format(self.hostname))
            print('The database port number is {}.'.format(self.port_num))
            print('The database instance is "{}".'.format(self.instance))
            print(self.get_connection_status())
        return
    # End of method print_all_connection_parameters.

    def sql_cmdline(self, os: str, sql: str) -> list:
        """ Run SQL against database using command line client.

        Parameters:
            os (str): 'Windows', 'Linux', or 'Darwin'
            sql (str): text of the SQL to run.
        Returns:
            sql_output (list): rows of output.
        """
        from subprocess import Popen, PIPE

        cmd = ''
        args = (self.username, self.password, self.hostname, self.port_num,
                self.instance)
        if self.db_type == access or self.db_client_exe == '':
            z = '{} DOES NOT HAVE A COMMAND LINE INTERFACE.'
            print(z.format(self.db_type).upper())
            return list()
        elif not is_file_in_path(os, self.db_client_exe):
            print('Did not find {} in PATH.'.format(self.db_client_exe))
            return list()
        elif self.db_type == mysql:
            conn_str = '--uri={}:{}@{}:{}/{}'.format(*args)
            cmd = [self.db_client_exe, conn_str, '--table', '--sql',
                   '--quiet-start']
        elif self.db_type == oracle:
            conn_str = '{}/{}@{}:{}/{}'.format(*args)
            cmd = [self.db_client_exe, conn_str]
        elif self.db_type == postgresql:
            conn_str = 'postgresql://{}:{}@{}:{}/{}'.format(*args)
            cmd = [self.db_client_exe, '-d', conn_str]
        elif self.db_type == sqlite:
            cmd = [self.db_client_exe, self.db_path]
        elif self.db_type == sqlserver:
            host_port = '{},{}'.format(self.hostname, self.port_num)
            cmd = [self.db_client_exe, '-U', self.username, '-P', self.password,
                   '-S', host_port, '-d', self.instance, '-s', "|"]
        else:
            print('Not yet implemented for {}.'.format(self.db_type))
            return list()
        p = Popen(cmd,  stdin=PIPE, stdout=PIPE, stderr=PIPE)
        (stdout, stderr) = p.communicate(sql.encode('utf-8'))
        stderr = stderr.decode('utf-8').split("\n")
        # MySQL warning to ignore.
        z = [('WARNING: Using a password on the command line interface can be '
              'insecure.'), '']
        if len(stderr) > 0 and stderr != [''] and stderr != z:
            print("PROBLEM IN SQL_CMDLINE:")
            print('cmd: ', cmd)
            print('sql: ', sql)
            print('stderr: ', stderr)
        return stdout.decode('utf-8').split("\n")
    # End of method sql_cmdline.

# End of Class DBInstance.
