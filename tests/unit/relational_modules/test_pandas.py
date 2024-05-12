#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tests for Pandas relational module

authors: Julian Minder
"""

import pytest

from data2neo.relational_modules.pandas import PandasSeriesResource, PandasDataFrameIterator
import pandas as pd
import pickle

@pytest.fixture
def example_dataframe():
    data = {
    "ID": [1,2,2,3,4,4],
    "FirstName": ["Julian", "Fritz",  "Fritz", "Hans", "Rudolfo", "Rudolfo"],
    "LastName": ["Minder", "Generic", "SomeGuy", "Müller", "Muster", "Muster"],
    "FavoriteFlower": ["virginica", "setosa", "setosa", "versicolor", "setosa", "setosa"]
    }
    return pd.DataFrame(data)

@pytest.fixture
def example_series(example_dataframe):
    return example_dataframe.iloc[0]
    
@pytest.fixture
def resource(example_series):
    return PandasSeriesResource(series=example_series, type="ExampleType")

def compare_resources(resource1, resource2):
    return str(resource1) == str(resource2) and (resource2.series == resource1.series).all() and resource2.supplies == resource1.supplies

class TestPandasSeriesResource:
    def test_attributes(self, resource, example_series):
        # Test attributes
        assert resource.type == "ExampleType"
        assert (resource.series == example_series).all()
    
    def test_getitem(self, resource, example_series):
        # Test get item
        for key, value in example_series.items():
            assert resource[key] == value

    def test_setitem(self, resource, example_series):
        # existing item
        resource[example_series.index[0]] = "Changed"
        assert resource[example_series.index[0]] == "Changed"
        # not existing item
        resource["NotExisting"] = "SomeValue"
        assert resource["NotExisting"] == "SomeValue"
    
    def test_repr(self, resource, example_series):
        assert str(resource) == f"PandasSeriesResource 'ExampleType' (row {example_series.name})"

    def test_supplies(self, resource):
        resource.supplies["test"] = 1
        assert resource.supplies["test"] == 1
    
    def test_pickling(self, resource):
        pickled_resource = pickle.dumps(resource)
        unpickled_resource = pickle.loads(pickled_resource)
        assert compare_resources(resource, unpickled_resource)

class TestPandasDataFrameIterator:
    @pytest.fixture
    def iterator(self, example_dataframe):
        return PandasDataFrameIterator(example_dataframe, type="ExampleType")



    def test_len(self, iterator, example_dataframe):
        assert len(iterator) == len(example_dataframe)

    def test_first(self, iterator, resource):
        iterator = iter(iterator)
        first_resource = next(iterator)
        assert first_resource.series.name == 0
        assert compare_resources(first_resource, resource)

    def test_next(self, iterator, resource):
        iterator = iter(iterator)
        first_resource = next(iterator)
        second_resource = next(iterator)
        assert not compare_resources(first_resource, second_resource)
        assert second_resource.series.name == 1
    
    def test_last(self, iterator):
        iterator = iter(iterator)
        for _ in range(6):
            next(iterator)
        with pytest.raises(StopIteration):
            next(iterator)

    def test_reset(self, iterator, resource):
        it = iter(iterator)
        for _ in range(6):
            next(it)
        with pytest.raises(StopIteration):
            next(it)
        it = iter(iterator)
        assert compare_resources(next(it), resource)
