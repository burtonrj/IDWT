from nosql.setup import global_init
from mongoengine.connection import disconnect
from datetime import datetime
from warnings import warn
import sqlite3
import os


class GlobalConfig:
    """
    Global configuration, stores global variables for logging and database access

    Properties
    ------------
    log: bool, (default = True)
        If True, logging is enabled
    log_path: str, (default = working directory)
        If log is True, path for logging text file
    db_type: str, (default = "nosql")
        What type of database to use: "nosql" or "sql"; "nosql" will use MongoDB, "sql" will use "SQLite3"
        "sql" is designed for local deployment only!
    db_connection: sqlite3.Connection
        If db_type = "sql", db_connection contains Connection object AFTER connect() method call
    """
    def __init__(self):
        self.log_path = f"{os.getcwd()}/IDWT_log_{datetime.now().date()}.txt"
        self.db_type = "nosql"
        self.db_connection = None
        self.db_alias = list()

    @staticmethod
    def _type_assertation(given: object,
                          expected: type):
        assert type(given) == expected, f"Expected type {expected}, got {type(given)}"

    def set_log_path(self, path: str):
        """
        Set log path parameter

        Parameters
        ----------
        path: str
            Must be valid path for logging text file

        Returns
        -------
        None
        """
        self._type_assertation(path, str)
        assert os.path.exists(os.path.dirname(os.path.abspath(path))), "Given path contains a directory that does not " \
                                                                       "exist, create directory before continuing"
        self.log_path = path

    def set_db_type(self, db_type: str):
        """
        Set db_type parameter. If currently connected to one or more databases, will raise ValueError.

        Parameters
        ----------
        db_type: str
            "nosql" or "sql"

        Returns
        -------
        None
        """
        self._type_assertation(db_type, str)
        assert db_type in ["nosql", "sql"], "Valid inputs for db_type are: 'nosql' or 'sql'"
        if self.db_connection is not None:
            raise ValueError("Currently connected to a local SQLite database, disconnect before changing "
                             "db_type")
        if self.db_alias:
            raise ValueError("Currently connected to one or more MongoDB instance(s), disconnect from "
                             "all databases before changing db_type")
        self.db_type = db_type

    def write_to_log(self, message: str):
        """
        Given some message, write new line to log file, prefixed with a timestamp

        Parameters
        ----------
        message: str
            Message for logging

        Returns
        -------
        None
        """
        log = open(self.log_path, mode="w")
        new_line = f"{datetime.now()}: {message} \n"
        log.write(new_line)
        log.close()

    def connect(self, db_name: str, alias: str = "core", **kwargs):
        """
        Create new database connection.
        SQL:
            Connects to an existing SQLite3 database or generates a new one in given path (db_name).
            Additional keyword arguments given as **kwargs are passed to sqlite3.connect
            (see: https://docs.python.org/3/library/sqlite3.html)
        NoSQL:
            Registers a new MongoDB connection. Connection details should be given in **kwargs and are passed to
            mongoengine.register_connection (see https://docs.mongoengine.org/guide/connecting.html)

        Parameters
        ----------
        db_name: str
            SQL: Should be the path to local SQLite database file
            NoSQL: Name of MongoDB database
        alias: str
            alias to use for new connection if db_type = "nosql"
        kwargs:
            Additional keyword arguments to pass to respective connection function

        Returns
        -------
        None
        """
        if self.db_type == "nosql":
            global_init(database_name=db_name,
                        alias=alias,
                        **kwargs)
            self.db_alias.append(alias)
        else:
            warn(f"Mode = SQL; could not locate SQLite database with path {db_name}, new database file will be "
                 f"generated")
            self.db_connection = sqlite3.connect(db_name, **kwargs)

    def close(self,
              alias: str = "core",
              close_all: bool = True):
        """
        Close database connections. If db_type = "sql", then connection to local sqlite database is closed.
        If db_type = "no_sql", then given alias is closed, or all connections are closed if all = True.

        Parameters
        ----------
        alias: str
            alias for mongodb instance to close
        close_all: bool
            If True, all mongodb connections are closed

        Returns
        -------
         None
        """
        if self.db_type == "nosql":
            if close_all:
                for alias in self.db_alias:
                    disconnect(alias=alias)
                self.db_alias = list()
            else:
                assert alias in self.db_alias, "No active connections with given alias"
                disconnect(alias=alias)
                self.db_alias = [x for x in self.db_alias if x != alias]
        else:
            if self.db_connection is not None:
                self.db_connection.close()


