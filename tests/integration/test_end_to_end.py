#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Integration tests for converting pandas dataframes to graphs.

authors: Julian Minder
"""

import pytest 

from rel2graph import Converter
from rel2graph.relational_modules.pandas import PandasDataframeIterator
from rel2graph import IteratorIterator
from mock_graph import MockGraph
from helpers import compare, update_matcher
from resources.data import no_duplicates, duplicates, before_update, person_only_nodes_only_result, schema_file_name
from resources.data import iris, flower_only_result, full_result

# Turn off reinstantiation warnings
Converter.no_instantiation_warnings = True

@pytest.fixture
def graph():
    return MockGraph()

@pytest.mark.parametrize(
    "workers",
    [1,5,20]
)
@pytest.mark.parametrize(
    "data,result",
    [(no_duplicates, person_only_nodes_only_result),
        (duplicates, person_only_nodes_only_result),
        (iris, flower_only_result)]
)
def test_single_type(graph, data, result, workers):
    iterator = PandasDataframeIterator(data[1], data[0])
    converter = Converter(schema_file_name, iterator, graph, num_workers=workers)
    update_matcher(graph) #REQUIRED to use mock matcher
    # run 
    converter()
    #compare
    compare(graph, result)


@pytest.mark.parametrize(
    "workers",
    [1,5,20]
)
@pytest.mark.parametrize(
    "initial_data,data,result",
    [(before_update, duplicates, person_only_nodes_only_result)]
)
def test_node_update(graph, initial_data, data, result, workers):
    # initial data
    iterator = PandasDataframeIterator(initial_data[1], initial_data[0])
    converter = Converter(schema_file_name, iterator, graph, num_workers=workers)
    update_matcher(graph) #REQUIRED to use mock matcher
    # run initial data
    converter()

    # updated data
    iterator = PandasDataframeIterator(data[1], data[0])
    converter = Converter(schema_file_name, iterator, graph, num_workers=workers)
    update_matcher(graph) #REQUIRED to use mock matcher
    # run updated data
    converter()
    #compare
    compare(graph, result)


@pytest.mark.parametrize(
    "workers",
    [1]
)
@pytest.mark.parametrize(
    "data_type_1,data_type_2,result",
    [(iris, duplicates, full_result)]
)
def test_two_types(graph, data_type_1, data_type_2, result, workers):
    iterator = IteratorIterator([
        PandasDataframeIterator(data_type_1[1], data_type_1[0]),
        PandasDataframeIterator(data_type_2[1], data_type_2[0])
    ])
    converter = Converter(schema_file_name, iterator, graph, num_workers=workers)
    update_matcher(graph) #REQUIRED to use mock matcher
    # run 
    converter()
    #compare
    compare(graph, result)
