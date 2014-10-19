#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, with_statement

import os
import copy
import time
import logging
import itertools

try:
    import MySQLdb
except ImportError:
    # If MySQLdb isn't available this module won't actually be useable,
    # but we want it to at least be importable on readthedocs.org,
    # which has limitations on third-party modules.
    if 'READTHEDOCS' in os.environ:
        MySQLdb = None
    else:
        raise

try:
    import pymssql
except ImportError:
    # If pymssql isn't available this module won't actually be useable,
    # but we want it to at least be importable on readthedocs.org,
    # which has limitations on third-party modules.
    if 'READTHEDOCS' in os.environ:
        MSSQLdb = None
    else:
        raise

version = "0.0.1"
version_info = (0, 0, 1)


class Connection(object):
    def __init__(self, config):

        self.dbtype = config.get("dbtype", None)
        self.host = config.get("host", None)
        self.database = config.get("database", None)
        self.charset = config.get("charset", "utf8")
        self.max_idle_time = float(config.get("max_idle_time", 7 * 3600))
        self.quiet_mode = config.get("quiet_mode", "False")

        args = dict(charset=self.charset)

        if config.get("user", None) is not None:
            args["user"] = config.get("user", None)

        if self.dbtype == "MySQL":
            """ initial MySQL required variables """
            args["db"] = self.database
            if config.get("passwd", None) is not None:
                args["passwd"] = config.get("passwd", None)

            self.sql_mode = config.get("sql_mode", "TRADITIONAL")

            args["use_unicode"] = True
            if "/" in self.host:
                args["unix_socket"] = self.host
            else:
                self.socket = None
                pair = self.host.split(":")
                if len(pair) == 2:
                    args["host"] = pair[0]
                    args["port"] = int(pair[1])
                else:
                    args["host"] = self.host
                    args["port"] = 3306

            # args["max_idle_time"] = self.max_idle_time
            args["sql_mode"] = config.get("sql_mode", "TRADITIONAL")
            args["connect_timeout"] = int(config.get("connect_timeout", 0))

        if self.dbtype == "MSSQL":
            """ initial MSSQL required variables. """

            args["database"] = self.database
            args["timeout"] = int(config.get("connect_timeout", 0))

            if config.get("passwd", None) is not None:
                args["password"] = config.get("passwd", None)

            if "/" in self.host:
                args["unix_socket"] = self.host
            else:
                self.socket = None
                pair = self.host.split(":")
                if len(pair) == 2:
                    args["host"] = pair[0]
                    args["port"] = int(pair[1])
                else:
                    args["host"] = self.host
                    args["port"] = 1433

        self._db = None
        self._db_args = args
        self._last_use_time = time.time()

        try:
            self.reconnect()
            if self.quiet_mode != "True":
                print "Connect to %s(%s: %s) successfully!" % (self.host, self.dbtype, self.database)
        except Exception, e:
            logging.error("Error connecting to %s(%s). ERROR: %s" % (self.host, self.dbtype, e.message), exc_info=True)


    def __del__(self):
        self.close()

    def close(self):
        if getattr(self, "_db", None) is not None:
            self._db.close()
            self._db = None

            if self.quiet_mode != "True":
                print "Close Connect to %s(%s: %s)." % (self.host, self.dbtype, self.database)

    def reconnect(self):
        """ Close the existing database connection and re-opens it. """
        self.close()

        if self.dbtype == "MySQL":
            self._db = MySQLdb.connect(**self._db_args)
        if self.dbtype == "MSSQL":
            self._db = pymssql.connect(**self._db_args)


    def iter(self, query, *parameters, **kwparameters):
        """
        :param query:
        :param parameters:
        :param kwparameters:
        :return: an iterator for the given query and parameters.
        """

        self._ensure_connected()

        if self.dbtype == "MySQL":
            cursor = MySQLdb.cursors.SSCursor(self._db)
        if self.dbtype == "MySQL":
            cursor = self._db.cursor()

        try:
            self._execute(cursor, query, parameters, kwparameters)
            column_names = [d[0] for d in cursor.description]
            for row in cursor:
                yield Row(zip(column_names, row))
        finally:
            cursor.close()


    def query(self, query, *parameters, **kwparameters):
        """
        :param query:
        :param parameters:
        :param kwparameters:
        :return: a row list for the given query and parameters
        """

        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters, kwparameters)
            colunm_names = [d[0] for d in cursor.description]
            return [Row(itertools.izip(colunm_names, row)) for row in cursor]
        finally:
            cursor.close()


    def get(self, query, *parameters, **kwparameters):
        """
        :param query:
        :param parameters:
        :param kwparameters:
        :return:
        """

        rows = self.query(query, *parameters, **kwparameters)
        if not rows:
            return None
        elif len(rows) > 1:
            raise Exception("Multiple rows returned for DataBase.get() query")
        else:
            return rows[0]


    def execute(self, query, *parameters, **kwparameters):
        return self.execute_lastrowid(query, *parameters, **kwparameters)

    def execute_lastrowid(self, query, *parameters, **kwparameters):
        """
        :param query:
        :param parameters:
        :param kwparameters:
        :return: Executes the given query, returning the lastrowid from the query
        """
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters, kwparameters)
            return cursor.lastrowid
        finally:
            cursor.close()

    def execute_rowcount(self, query, *parameters, **kwparameters):
        """
        :param query:
        :param parameters:
        :param kwparameters:
        :return: Executes the given query, returning the rowcount from the query.
        """
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters, kwparameters)
            return cursor.rowcount
        finally:
            cursor.close()

    def executemany(self, query, parameters):
        """
        :param query:
        :param parameters:
        :return: Executes the given query against all the given para sequences. We return teh lastrowid from the query
        """

        return self.execute_lastrowid(self, query, parameters)

    def executemany_lastrowid(self, query, parameters):
        """
        :param query:
        :param parameters:
        :return: Executes the given query against all the given para sequences. We return teh lastrowid from the query
        """
        cursor = self._cursor()
        try:
            self._executemany(cursor, query, parameters)
            return cursor.lastrowid
        finally:
            cursor.close()

    def executemany_rowcount(self, query, parameters):
        """
        :param query:
        :param parameters:
        :return: Executes the given query against all the given para sequences. We return teh rowcount from the query
        """
        cursor = self._cursor()
        try:
            self._executemany(cursor, query, parameters)
            return cursor.rowcount
        finally:
            cursor.close()

    update = execute_rowcount
    updatemany = executemany_rowcount

    insert = execute_lastrowid
    insertmany = executemany_lastrowid


    def _execute(self, cursor, query, parameters, kwparameters):
        try:
            return cursor.execute(query, kwparameters or parameters)
        except MySQLdb.OperationalError, e:
            logging.error("Error connecting to %s(%s). ERROR: %s" % (self.host, self.dbtype, e.message))
            self.close()
            raise

    def _cursor(self):
        self.__ensure_connected()
        return self._db.cursor()


    def __ensure_connected(self):
        if (self._db is None or (time.time() - self._last_use_time > self.max_idle_time)):
            self.reconnect()
        self._last_use_time = time.time()


class Row(dict):
    """ A dict that allows for object-like property access syntax """

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)