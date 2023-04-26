#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Implementation for pandas as the relational module. 

authors: Julian Minder
"""

from typing import List, Iterable, Dict, Any
from .. import ResourceIterator
from .. import Resource
import sqlite3
from collections import defaultdict
from queue import Queue
import logging

logger = logging.getLogger(__name__)


class SQLiteResource(Resource):
    """Implementation of the sqlite Resource. Enables access to a row of a sqlite table"""

    def __init__(self, data: Iterable, cols: List[str], pks: List[str], table: str) -> None:
        """Wraps a row of a sqlite table and is used as input data object for the converter. May hold additional supplies to pass data between factories.

        Args:
            data: Wrapped row of a sqlite table.
            cols: List of column names.
            pks: List of primary keys.
            table: Name of table that this row is an entity of.
        """
        super().__init__()
        self._data = list(data)
        self._type = table
        # convert to dict for fast access
        self._cols = dict((col, i) for i,col in enumerate(cols))
        self._pks = tuple(pks)

    @property
    def type(self) -> str:
        """Returns the type of the resource. Is used to select correct factory"""
        return self._type    
    
    @property
    def pks(self) -> Dict[str, Any]:
        """Returns the primary keys of the resource"""
        return dict((pk, self[pk]) for pk in self._pks)
    
    @property
    def cols(self) -> List[str]:
        """Returns the columns of the resource"""
        return list(self._cols.keys())


    def __getitem__(self, key: str) -> str:
        """ 
        Gets the value with key 'key'. 
        """
        return self._data[self._cols[key]]

    def __setitem__(self, key: str, value: Any) -> None:
        """
        Sets the value of with key 'key'.
        """
        if key in self._cols.keys():
            self._data[self._cols[key]] = value
        else:
            self._cols[key] = len(self._data)
            self._data.append(value)
        
    def __repr__(self) -> str:
        """
        Gets a string representation of the resource. Only used for logging.
        """
        repr = f"{super().__repr__()} ("
        for pk in self._pks:
            repr += f"{pk}={self[pk]},"
        return repr + ")"

class SQLiteIterator(ResourceIterator):
    """ Implementation of the sqlite ResourceIterator. Enables iteration over a sqlite database. """

    def __init__(self, database: sqlite3.Connection, filter: List[str] = None, primary_keys: Dict[str, List[str]] = None, mix_tables: bool = True, buffer_size: int = 5000, lock: "Lock" = None) -> None:
        """Initializes the iterator. Opens a connection to the database and saves the tables that should be iterated over.

        Args:
            database: Sqlite database connection to iterate over.
            filter: List of tables to iterate over. If None all tables in database are used. Defaults to None.
            primary_keys: Dict of primary keys for each table. If None the primary keys are determined automatically. Defaults to None.
            mix_tables: If True, the iterator will mix the tables. If False, the iterator will iterate over all tables in order. Defaults to True.
            buffer_size: Size of the buffer for each table. Defaults to 5000.
            lock: Lock object to synchronize the access to the sqlite database. Defaults to None.
        Raises:
            Exception: If no primary key is found for a table.
        """
        super().__init__()
        assert isinstance(database, sqlite3.Connection), "Please pass a sqlite3.Connection object to the iterator."
        
        self._con = database
        self._con_lock = lock

        all_tables = [table[0] for table in self._con.execute(
            'SELECT name from sqlite_master where type= "table"'
            ).fetchall()]
        
        self._tables = all_tables
        if filter is not None:
            self._tables = filter
        
        logger.info(f"Iterating over tables: {self._tables}")

        self._cols = {}
        self._pks = {}
        try:
            if self._con_lock is not None:
                self._con_lock.acquire()
            for table in self._tables:
                cols = self._con.execute(f"PRAGMA table_info('{table}');").fetchall()
                self._cols[table] = [col[1] for col in cols]
                if primary_keys is not None and table in primary_keys.keys():
                    self._pks[table] = primary_keys[table]
                else:
                    self._pks[table] = [col[1] for col in cols if col[5]]
                if len(self._pks[table]) == 0:
                    raise ValueError(f"Table '{table}' has no primary key, which is required for the conversion. Please add a primary key to the table or specify it manually when instatiating the Iterator.")
        finally:
            if self._con_lock is not None:
                self._con_lock.release()
        self._cursors = {}

        self._len = None
        self._mix_tables = mix_tables
        self._buffer_size = buffer_size
    

    def _init_cursors(self):
        """Initiates the cursors for all tables."""
        if self._con_lock is not None:
            self._con_lock.acquire()
        try:
            for table in self._tables:
                self._cursors[table] = self._con.execute(f"SELECT * FROM {table}")
        finally:
            if self._con_lock is not None:
                self._con_lock.release()

    def __iter__(self):
        """Returns an iterator over the database."""
        self._init_cursors()
        buffer = defaultdict(Queue)
        while True:
            for key in self._cursors.keys():
                break_flag = False
                while(True):
                    try:
                        yield buffer[key].get_nowait()
                    except GeneratorExit:
                        return
                    except:
                        # Fetch next 5000 rows
                        if self._con_lock is not None:
                            self._con_lock.acquire()
                        try:
                            res = self._cursors[key].fetchmany(5000)
                        finally:
                            if self._con_lock is not None:
                                self._con_lock.release()
                        for row in res:
                            buffer[key].put(SQLiteResource(row, self._cols[key], self._pks[key], key))
                        if buffer[key].empty():
                            # Remove cursor from dict
                            del self._cursors[key]
                            if len(self._cursors) == 0:
                                return
                            break_flag = True
                            break
                    if self._mix_tables:
                        break
                if break_flag:
                    break
                
        
    def __len__(self) -> None:
        """ Returns the number of resources in the database."""
        if self._len is None:
            self._len = 0
            if self._con_lock is not None:
                self._con_lock.acquire()
            try:
                for table in self._tables:
                    self._len += self._con.execute(f"SELECT Count(*) FROM {table}").fetchone()[0]
            finally:
                if self._con_lock is not None:
                    self._con_lock.release()
        return self._len
    