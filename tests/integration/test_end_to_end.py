#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Integration tests for converting pandas dataframes to graphs.

authors: Julian Minder
"""

import pytest 

from rel2graph import Converter
from rel2graph.relational_modules.pandas import PandasDataframeIterator
from rel2graph.utils import load_file
from rel2graph import IteratorIterator
from rel2graph import register_subgraph_postprocessor

from mock_graph import MockGraph
from helpers import compare, update_matcher, StateRecoveryException
from resources.data_end_to_end import no_duplicates, duplicates, before_update, person_only_nodes_only_result, schema_file_name
from resources.data_end_to_end import iris, flower_only_result, full_result, result_parallel
import pandas as pd
import time


# Turn off reinstantiation warnings
Converter.no_instantiation_warnings = True

# Set logging level
import logging
logging.getLogger("rel2graph").setLevel(logging.DEBUG)


@pytest.fixture
def graph():
    return MockGraph()

@pytest.fixture
def graph_wpr():
    graph = MockGraph()
    graph.allow_parallel_relations = True
    return graph

@pytest.mark.parametrize("workers",[1,5,20])
@pytest.mark.parametrize(
    "data,result",
    [(no_duplicates, person_only_nodes_only_result),
        (duplicates, person_only_nodes_only_result),
        (iris, flower_only_result)]
)
def test_single_type(graph, data, result, workers):
    iterator = PandasDataframeIterator(data[1], data[0])
    converter = Converter(load_file(schema_file_name), iterator, graph, num_workers=workers)
    update_matcher(graph) #REQUIRED to use mock matcher
    # run 
    converter()
    #compare
    compare(graph, result)


@pytest.mark.parametrize("workers",[1,5,20])
@pytest.mark.parametrize(
    "initial_data,data,result",
    [(before_update, duplicates, person_only_nodes_only_result)]
)
def test_node_update(graph, initial_data, data, result, workers):
    # initial data
    iterator = PandasDataframeIterator(initial_data[1], initial_data[0])
    converter = Converter(load_file(schema_file_name), iterator, graph, num_workers=workers)
    update_matcher(graph) #REQUIRED to use mock matcher
    # run initial data
    converter()

    # updated data
    iterator = PandasDataframeIterator(data[1], data[0])
    converter = Converter(load_file(schema_file_name), iterator, graph, num_workers=workers)
    update_matcher(graph) #REQUIRED to use mock matcher
    # run updated data
    converter()
    #compare
    compare(graph, result)


@pytest.mark.parametrize("workers",[1, 5, 20])
@pytest.mark.parametrize(
    "data_type_1,data_type_2,result",
    [(iris, duplicates, full_result)]
)
def test_two_types(graph, data_type_1, data_type_2, result, workers):
    iterator = IteratorIterator([
        PandasDataframeIterator(data_type_1[1], data_type_1[0]),
        PandasDataframeIterator(data_type_2[1], data_type_2[0])
    ])
    converter = Converter(load_file(schema_file_name), iterator, graph, num_workers=workers)
    update_matcher(graph) #REQUIRED to use mock matcher
    # run 
    converter()
    #compare
    compare(graph, result)


@pytest.mark.parametrize("workers",[1,5,20])
@pytest.mark.parametrize(
    "data_type_1,data_type_2,result",
    [(iris, duplicates, full_result)]
)
def test_state_recovery(graph, data_type_1, data_type_2, result, workers):
    iterator = IteratorIterator([
        PandasDataframeIterator(data_type_1[1], data_type_1[0]+"StateRecovery"),
        PandasDataframeIterator(data_type_2[1], data_type_2[0]+"StateRecovery")
    ])
    converter = Converter(load_file(schema_file_name), iterator, graph, num_workers=workers)
    update_matcher(graph) #REQUIRED to use mock matcher
    # run (we require 3 runs)
    for i in range(4):
        try:
            converter()
        except StateRecoveryException:
            pass
    #compare
    compare(graph, result)

@pytest.mark.parametrize("workers",[1,5,20])
@pytest.mark.parametrize(
    "data_type_1,data_type_2,result",
    [(iris, no_duplicates, result_parallel)]
)
def test_parallel_relations(graph_wpr, data_type_1, data_type_2, result, workers):
    iterator = IteratorIterator([
        PandasDataframeIterator(data_type_1[1], data_type_1[0]+"Parallel"),
        PandasDataframeIterator(data_type_2[1], data_type_2[0]+"Parallel")
    ])
    converter = Converter(load_file(schema_file_name), iterator, graph_wpr, num_workers=workers)
    update_matcher(graph_wpr) #REQUIRED to use mock matcher
    converter()
    #compare
    compare(graph_wpr, result)


# Serialization Test

class StopException(Exception):
    pass

@register_subgraph_postprocessor
def DELAY_AND_MAYBE_EXIT(subgraph):
    time.sleep(0.1)
    if subgraph.nodes[0]["ID"] == 5:
        raise StopException()
    return subgraph

def test_serialize(graph):
    """Tests serialisation."""
    data = pd.DataFrame({"ID": list(range(10)), "next": list(range(1,11))})
    result = {
        "nodes": [(["Entity"], {"ID": i}) for i in range(5)],
        "relations": []
    }
    iterator = PandasDataframeIterator(data, "Entity")
    converter = Converter(load_file(schema_file_name), iterator, graph, serialize=True)
    update_matcher(graph) #REQUIRED to use mock matcher
    try:
        converter()
    except StopException:
        pass
    compare(graph, result)

def test_raise_when_serialize_and_multiple_workers(graph):
    """Tests the exception when using the serialize options as well as specifying multiple workers."""
    data = pd.DataFrame({"ID": list(range(10)), "next": list(range(1,11))})
    iterator = PandasDataframeIterator(data, "Entity")
    update_matcher(graph) #REQUIRED to use mock matcher
    with pytest.raises(ValueError) as excinfo:
        converter = Converter(load_file(schema_file_name), iterator, graph, serialize=True, num_workers=10)
    exception_msg = excinfo.value.args[0]
    assert exception_msg == "You can't use serialization and parallel processing (num_workers > 1) at the same time."
