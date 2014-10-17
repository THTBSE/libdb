#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'yan9yu'

from __future__ import absolute_import, division, with_statement

import os
import copy
import time
import logging
import itertools

try:
    import MySQLdb.constants
    import MySQLdb.converters
    import MySQLdb.cursors
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
    # If MySQLdb isn't available this module won't actually be useable,
    # but we want it to at least be importable on readthedocs.org,
    # which has limitations on third-party modules.
    if 'READTHEDOCS' in os.environ:
        MySQLdb = None
    else:
        raise

version = "0.0.1"
version_info = (0, 0, 1)


class Connection(object):
    def __init__(self, config):
        self.config = config
        self.dbtype = config.get("dbtype", None)
        self.host = config.get("host", None)
        self.database = config.get("database", None)
        self.charset = config.get("charset", "utf8")
        self.max_idle_time = float(config.get("max_idle_time", 7 * 3600))
        self.connect_timeout = int(config.get("connect_timeout", 0))
        self.sql_mode = config.get("sql_mode", "TRADITIONAL")
        self.quiet_mode = config.get("quiet_mode", "False")

        args = dict(use_unicode=True, charset=self.charset, db=self.database, connect_timeout=self.connect_timeout,
                    sql_mode=self.sql_mode)

        if config.get("user", None) is not None:
            args["user"] = config.get("user", None)

        if config.get("password", None) is not None:
            args["password"] = config.get("password", None)

        # if "/" in self.host:
        # args["unix_socket"] = self.host
        # else:
        # self.socket = None
        # pair = self.host.split(":")
        # if len(pair) == 2:
        # args["host"] = pair[0]
        # args["port"] = int(pair[1])
        #     else:
        #         args["host"] = self.host
        #         args["port"] = 3306

        if self.dbtype == "MySQL":
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

        if self.dbtype == "MSSQL":
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
        except Exception:
            logging.error("Cannot connect to %s(%s)" % (self.host, self.dbtype), exc_info=True)


    def __del__(self):
        self.close()

    def close(self):
        if getattr(self, "_db", None) is not None:
            self._db.close()
            self._db = None

            if self.quiet_mode != "True":
                print "Close Connect to %s(%s: %s)." % (self.host, self.dbtype, self.database)

    def reconnect(self):
        """
        Close the existing database connection and re-opens it.
        """
        self.close()

        if self.dbtype == "MySQL":
            self._db = MySQLdb.connect(**self._db_args)
        if self.dbtype == "MSSQL":
            self._db = pymssql.connect(**self._db_args)


















