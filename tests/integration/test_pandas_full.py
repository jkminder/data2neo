#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Integration tests for converting pandas dataframes to graphs.

authors: Julian Minder
"""

import pytest 

from rel2graph import Converter
from rel2graph.relational_modules.pandas import PandasDataframeIterator
from rel2graph.core.factories.matcher import Matcher
import pandas as pd

from mock_graph import MockGraph
from helpers import compare, update_matcher

# Turn off reinstantiation warnings
Converter.no_instantiation_warnings = True

### Helper ###
def update_matcher(graph):
    """Hack to use mock matcher"""
    Matcher.graph_matcher = graph.matcher

## test input data ##
schema_file_name = "tests/integration/resources/test_pandas_schema.yaml"

no_duplicates = ("Person", pd.DataFrame({
    "ID": [1,2,3,4],
    "FirstName": ["Julian", "Fritz",  "Hans", "Rudolfo"],
    "LastName": ["Minder", "SomeGuy", "Müller",  "Muster"],
    "FavoriteFlower": ["virginica", "setosa", "versicolor", "setosa"]
}))

duplicates = ("Person", pd.DataFrame({
        "ID": [1,2,2,3,4,4,4,4],
        "FirstName": ["Julian", "Fritz",  "Fritz", "Hans", "Rudolfo", "Rudolfo", "Rudolfo", "Rudolfo"],
        "LastName": ["Minder", "SomeGuy", "SomeGuy", "Müller", "Muster", "Muster", "Muster", "Muster"],
        "FavoriteFlower": ["virginica", "setosa", "setosa", "versicolor", "setosa", "setosa", "setosa", "setosa"]
}))

duplicates_update = ("Person", pd.DataFrame({
        "ID": [1,2,2,3,4,4],
        "FirstName": ["Julian", "Fritz",  "Fritz", "Hans", "Rudolfo", "Rudolfo"],
        "LastName": ["Minder", "Generic", "SomeGuy", "Müller", "Muster", "Muster"],
        "FavoriteFlower": ["virginica", "setosa", "setosa", "versicolor", "setosa", "setosa"]
}))

iris = pd.read_csv('https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv')

## test result data ##
nodes_only_result = {
    "nodes": [
        (["Person"], {"ID": "1", "FirstName": "Julian", "LastName": "Minder"}),
        (["Person"], {"ID": "2", "FirstName": "Fritz", "LastName": "SomeGuy"}),
        (["Person"], {"ID": "3", "FirstName": "Hans", "LastName": "Müller"}),
        (["Person"], {"ID": "4", "FirstName": "Rudolfo", "LastName": "Muster"})
    ], "relations": []
}
######################


@pytest.fixture
def graph():
    return MockGraph()

@pytest.mark.parametrize(
    "data,result,workers",
    [(no_duplicates, nodes_only_result,1),
        (duplicates, nodes_only_result,1),
        (duplicates_update, nodes_only_result,1),
        (no_duplicates, nodes_only_result,5),
        (duplicates, nodes_only_result,5),
        (duplicates_update, nodes_only_result,5)]
)
def test_end_to_end(graph, data, result, workers):
    iterator = PandasDataframeIterator(data[1], data[0])
    converter = Converter(schema_file_name, iterator, graph, num_workers=workers)
    update_matcher(graph) #REQUIRED to use mock matcher
    # run 
    converter()
    #compare
    compare(graph, result)

