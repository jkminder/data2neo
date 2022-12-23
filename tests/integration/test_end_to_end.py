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
from rel2graph.py2neo_extensions import GraphWithParallelRelations
import rel2graph.common_modules

from helpers import compare
from resources.data_end_to_end import no_duplicates, duplicates, before_update, person_only_nodes_only_result, schema_file_name
from resources.data_end_to_end import iris, flower_only_result, full_result, result_parallel
import pandas as pd
import time
from py2neo import Graph
import multiprocessing as mp

# Set logging level
import logging
logging.getLogger("rel2graph").setLevel(logging.DEBUG)


@pytest.fixture
def graph():
    graph = Graph()
    graph.delete_all()
    return graph

@pytest.fixture
def graph_wpr():
    graph = GraphWithParallelRelations()
    graph.delete_all()
    return graph

@pytest.mark.parametrize("workers",[1,5])
@pytest.mark.parametrize("batch_size",[1,100])
@pytest.mark.parametrize(
    "data,result",
    [(no_duplicates, person_only_nodes_only_result),
        (duplicates, person_only_nodes_only_result),
        (iris, flower_only_result)]
)
def test_single_type(graph, data, result, workers, batch_size):
    iterator = PandasDataframeIterator(data[1], data[0])
    converter = Converter(load_file(schema_file_name), iterator, graph, num_workers=workers,  batch_size=batch_size)
    # run 
    converter()
    #compare
    compare(graph, result)


@pytest.mark.parametrize("workers",[1,5])
@pytest.mark.parametrize("batch_size",[1,100])
@pytest.mark.parametrize(
    "initial_data,data,result",
    [(before_update, duplicates, person_only_nodes_only_result)]
)
def test_node_update(graph, initial_data, data, result, workers, batch_size):
    # initial data
    iterator = PandasDataframeIterator(initial_data[1], initial_data[0])
    converter = Converter(load_file(schema_file_name), iterator, graph, num_workers=workers,  batch_size=batch_size)
    # run initial data
    converter()

    # updated data
    iterator = PandasDataframeIterator(data[1], data[0])
    converter = Converter(load_file(schema_file_name), iterator, graph, num_workers=workers,  batch_size=batch_size)
    # run updated data
    converter()
    #compare
    compare(graph, result)


@pytest.mark.parametrize("workers",[1, 5])
@pytest.mark.parametrize("batch_size",[1,100])
@pytest.mark.parametrize(
    "data_type_1,data_type_2,result",
    [(iris, duplicates, full_result)]
)
def test_two_types(graph, data_type_1, data_type_2, result, workers, batch_size):
    iterator = IteratorIterator([
        PandasDataframeIterator(data_type_1[1], data_type_1[0]),
        PandasDataframeIterator(data_type_2[1], data_type_2[0])
    ])
    converter = Converter(load_file(schema_file_name), iterator, graph, num_workers=workers,  batch_size=batch_size)
    # run 
    converter()
    #compare
    compare(graph, result)


@pytest.mark.parametrize("workers",[1,5])
@pytest.mark.parametrize("batch_size",[1,100])
@pytest.mark.parametrize(
    "data_type_1,data_type_2,result",
    [(iris, no_duplicates, result_parallel)]
)
def test_parallel_relations(graph_wpr, data_type_1, data_type_2, result, workers, batch_size):
    iterator = IteratorIterator([
        PandasDataframeIterator(data_type_1[1], data_type_1[0]+"Parallel"),
        PandasDataframeIterator(data_type_2[1], data_type_2[0]+"Parallel")
    ])
    converter = Converter(load_file(schema_file_name), iterator, graph_wpr, num_workers=workers,  batch_size=batch_size)
    converter()
    #compare
    compare(graph_wpr, result)

# Exeption tests
counter = 0
@register_subgraph_postprocessor
def RAISE_ERROR(subgraph):
    global counter 
    counter += 1
    if counter == 3:
        raise ValueError("This is an error")
    return subgraph

def test_exception(graph):
    """Tests exception handling."""
    iterator = PandasDataframeIterator(no_duplicates[1], no_duplicates[0] + "RaiseError")

    converter = Converter(load_file(schema_file_name), iterator, graph, serialize=True)
    with pytest.raises(ValueError):
        converter()
    
# Serialization Test

class CancelException(Exception):
    pass

@register_subgraph_postprocessor
def DELAY_AND_MAYBE_EXIT(subgraph):
    # test whether we are in the main process
    if mp.current_process().name != "MainProcess":
        raise ValueError("This should not be run in a subprocess")
    time.sleep(0.1)
    if subgraph.nodes[0]["ID"] == 5:
        raise CancelException()
    return subgraph

def test_serialize(graph):
    """Tests serialisation."""
    data = pd.DataFrame({"ID": list(range(10)), "next": list(range(1,11))})
    result = {
        "nodes": [(["Entity"], {"ID": i}) for i in range(5)],
        "relations": []
    }
    iterator = PandasDataframeIterator(data, "Entity")
    # We run with batchsize 1 to make sure that the serialization is actually used
    converter = Converter(load_file(schema_file_name), iterator, graph, serialize=True, batch_size=1)
    try:
        converter()
    except CancelException:
        pass
    compare(graph, result)

def test_raise_when_serialize_and_multiple_workers(graph):
    """Tests the exception when using the serialize options as well as specifying multiple workers."""
    data = pd.DataFrame({"ID": list(range(10)), "next": list(range(1,11))})
    iterator = PandasDataframeIterator(data, "Entity")
    with pytest.raises(ValueError) as excinfo:
        converter = Converter(load_file(schema_file_name), iterator, graph, serialize=True, num_workers=10)
    exception_msg = excinfo.value.args[0]
    assert exception_msg == "You can't use serialization and parallel processing (num_workers > 1) at the same time."
