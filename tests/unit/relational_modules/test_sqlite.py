#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tests for Pandas relational module

authors: Julian Minder
"""

import pytest

from rel2graph.relational_modules.sqlite import SQLiteResource, SQLiteIterator
import sqlite3
import pickle

@pytest.fixture
def example_database():
    conn = sqlite3.connect(":memory:")
    c = conn.cursor()
    c.execute("CREATE TABLE ExampleTable (ID INTEGER PRIMARY KEY, FirstName TEXT, LastName TEXT, FavoriteFlower TEXT)")
    c.execute("INSERT INTO ExampleTable VALUES (1, 'Julian', 'Minder', 'virginica')")
    c.execute("INSERT INTO ExampleTable VALUES (2, 'Fritz', 'Generic', 'setosa')")
    c.execute("INSERT INTO ExampleTable VALUES (3, 'Fritz', 'SomeGuy', 'setosa')")
    c.execute("INSERT INTO ExampleTable VALUES (4, 'Hans', 'Müller', 'versicolor')")
    c.execute("INSERT INTO ExampleTable VALUES (5, 'Rudolfo', 'Muster', 'setosa')")
    c.execute("INSERT INTO ExampleTable VALUES (6, 'Rudolfo', 'Muster', 'setosa')")

    # Create a second dummy table
    c.execute("CREATE TABLE ExampleTable2 (ID INTEGER PRIMARY KEY, FirstName TEXT, LastName TEXT, FavoriteFlower TEXT)")
    c.execute("INSERT INTO ExampleTable2 VALUES (7, 'Julian', 'Minder', 'virginica')")
    conn.commit()
    return conn

@pytest.fixture
def example_row(example_database):
    # get first row
    c = example_database.cursor()
    c.execute("SELECT * FROM ExampleTable LIMIT 1")
    cols = [column[0] for column in c.description]
    return c.fetchone(), cols
        
@pytest.fixture
def resource(example_row):
    return SQLiteResource(data=example_row[0], cols=example_row[1], pks=["ID"], table="ExampleTable")

def compare_resources(resource1, resource2):
    return str(resource1) == str(resource2) and [resource2[col] for col in resource2.cols] == [resource1[col] for col in resource1.cols]


class TestSQLiteResource:
    def test_attributes(self, resource, example_row):
        # Test attributes
        assert resource.type == "ExampleTable"
        assert (resource.pks == {"ID":1})
        assert (resource.cols == example_row[1])
    
    def test_getitem(self, resource, example_row):
        # Test get item
        for key, value in zip(example_row[1], example_row[0]):
            assert resource[key] == value

    def test_setitem(self, resource, example_row):
        # existing item
        resource[example_row[1][0]] = "Changed"
        assert resource[example_row[1][0]] == "Changed"
        # not existing item
        resource["NotExisting"] = "SomeValue"
        assert resource["NotExisting"] == "SomeValue"
    
    def test_repr(self, resource, example_row):
        assert str(resource) == f"SQLiteResource 'ExampleTable' (ID={example_row[0][0]},)"

    def test_supplies(self, resource):
        resource.supplies["test"] =  1
        assert resource.supplies["test"] == 1
    
    def test_pickling(self, resource):
        pickled_resource = pickle.dumps(resource)
        unpickled_resource = pickle.loads(pickled_resource)
        assert compare_resources(unpickled_resource, resource)

class TestSQLiteIterator:
    def test_len(self, example_database):
        iterator = SQLiteIterator(example_database)
        assert len(iterator) == 7
        iterator = SQLiteIterator(example_database, filter=["ExampleTable"])
        assert len(iterator) == 6
        iterator = SQLiteIterator(example_database, filter=["ExampleTable2"])
        assert len(iterator) == 1
        iterator = SQLiteIterator(example_database, filter=["ExampleTable", "ExampleTable2"])
        assert len(iterator) == 7

    def test_first(self, example_database, resource):
        iterator = SQLiteIterator(example_database)
        iterator = iter(iterator)
        first_resource = next(iterator)
        assert compare_resources(first_resource, resource)

    def test_mixing(self, example_database):
        iterator = SQLiteIterator(example_database, mix_tables=True)
        iterator = iter(iterator)
        first_resource = next(iterator)
        second_resource = next(iterator)
        assert first_resource["ID"] == 1
        assert second_resource["ID"] == 7
        iterator = SQLiteIterator(example_database, mix_tables=False)
        iterator = iter(iterator)
        first_resource = next(iterator)
        second_resource = next(iterator)
        assert first_resource["ID"] == 1
        assert second_resource["ID"] == 2

    def test_filter(self, example_database):
        iterator = SQLiteIterator(example_database, filter=["ExampleTable"])
        iterator = iter(iterator)
        first_resource = next(iterator)
        assert first_resource["ID"] == 1

        iterator = SQLiteIterator(example_database, filter=["ExampleTable2"])
        iterator = iter(iterator)
        first_resource = next(iterator)
        assert first_resource["ID"] == 7

    def test_primary_keys(self, example_database):
        iterator = SQLiteIterator(example_database)
        iterator = iter(iterator)
        first_resource = next(iterator)
        assert first_resource.pks == {"ID":1}

        iterator = SQLiteIterator(example_database, primary_keys={"ExampleTable": ["ID", "FirstName"]})
        iterator = iter(iterator)
        first_resource = next(iterator)
        assert first_resource.pks == {"ID":1, "FirstName": "Julian"}

    def test_next(self, example_database, resource):
        iterator = SQLiteIterator(example_database)

        iterator = iter(iterator)
        first_resource = next(iterator)
        second_resource = next(iterator)
        assert not compare_resources(first_resource, second_resource)
        assert second_resource["ID"] == 7
    
    def test_last(self, example_database):
        iterator = SQLiteIterator(example_database)

        iterator = iter(iterator)
        for _ in range(7):
            next(iterator)
        with pytest.raises(StopIteration):
            next(iterator)

    def test_reset(self, example_database, resource):
        iterator = SQLiteIterator(example_database)

        it = iter(iterator)
        for _ in range(7):
            next(it)
        with pytest.raises(StopIteration):
            next(it)
        it = iter(iterator)
        assert compare_resources(next(it), resource)
