#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Integration tests for converting pandas dataframes to graphs.

authors: Julian Minder
"""

import pytest 

from rel2graph import Converter
from rel2graph.relational_modules.pandas import PandasDataFrameIterator
from rel2graph.utils import load_file
from rel2graph import IteratorIterator
from rel2graph import register_subgraph_postprocessor
from rel2graph.common_modules import MERGE_RELATIONSHIPS

from resources.data_end_to_end import no_duplicates, duplicates, before_update, person_only_nodes_only_result, schema_file_name
from resources.data_end_to_end import iris, flower_only_result, full_result, result_parallel
import pandas as pd
import time
import multiprocessing as mp

from helpers import *

# Set logging level
import logging
logging.getLogger("rel2graph").setLevel(logging.DEBUG)


@pytest.mark.parametrize("workers",[1,5])
@pytest.mark.parametrize("batch_size",[1,100])
@pytest.mark.parametrize(
    "data,result",
    [(no_duplicates, person_only_nodes_only_result),
        (duplicates, person_only_nodes_only_result),
        (iris, flower_only_result)]
)
def test_single_type(data, result, workers, batch_size, session, uri, auth):
    iterator = PandasDataFrameIterator(data[1], data[0])
    converter = Converter(load_file(schema_file_name), iterator, uri, auth, num_workers=workers,  batch_size=batch_size)
    # run 
    converter()
    #compare
    compare(session, result)


@pytest.mark.parametrize("workers",[1,5])
@pytest.mark.parametrize("batch_size",[1,100])
@pytest.mark.parametrize(
    "initial_data,data,result",
    [(before_update, duplicates, person_only_nodes_only_result)]
)
def test_node_update(initial_data, data, result, workers, batch_size, session, uri, auth):
    # initial data
    iterator = PandasDataFrameIterator(initial_data[1], initial_data[0])
    converter = Converter(load_file(schema_file_name), iterator, uri, auth, num_workers=workers,  batch_size=batch_size)
    # run initial data
    converter()

    # updated data
    iterator = PandasDataFrameIterator(data[1], data[0])
    converter = Converter(load_file(schema_file_name), iterator, uri, auth, num_workers=workers,  batch_size=batch_size)
    # run updated data
    converter()
    #compare
    compare(session, result)


@pytest.mark.parametrize("workers",[1, 5])
@pytest.mark.parametrize("batch_size",[1,100])
@pytest.mark.parametrize(
    "data_type_1,data_type_2,result",
    [(iris, duplicates, full_result)]
)
def test_two_types(data_type_1, data_type_2, result, workers, batch_size, session, uri, auth):
    iterator = IteratorIterator([
        PandasDataFrameIterator(data_type_1[1], data_type_1[0]),
        PandasDataFrameIterator(data_type_2[1], data_type_2[0])
    ])
    converter = Converter(load_file(schema_file_name), iterator, uri, auth, num_workers=workers,  batch_size=batch_size)
    # run 
    converter()
    #compare
    compare(session, result)


@pytest.mark.parametrize("workers",[1,5])
@pytest.mark.parametrize("batch_size",[1,100])
@pytest.mark.parametrize(
    "data_type_1,data_type_2,result",
    [(iris, no_duplicates, result_parallel)]
)
def test_parallel_relations(data_type_1, data_type_2, result, workers, batch_size, session, uri, auth):
    iterator = IteratorIterator([
        PandasDataFrameIterator(data_type_1[1], data_type_1[0]+"Parallel"),
        PandasDataFrameIterator(data_type_2[1], data_type_2[0]+"Parallel")
    ])
    converter = Converter(load_file(schema_file_name), iterator, uri, auth, num_workers=workers,  batch_size=batch_size)
    converter()
    #compare
    compare(session, result)

# Exeption tests
counter = 0
@register_subgraph_postprocessor
def RAISE_ERROR(subgraph):
    global counter 
    counter += 1
    if counter == 3:
        raise ValueError("This is an error")
    return subgraph

def test_exception(session, uri, auth):
    """Tests exception handling."""
    iterator = PandasDataFrameIterator(no_duplicates[1], no_duplicates[0] + "RaiseError")

    converter = Converter(load_file(schema_file_name), iterator, uri, auth, serialize=True)
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

def test_serialize(session, uri, auth):
    """Tests serialisation."""
    data = pd.DataFrame({"ID": list(range(10)), "next": list(range(1,11))})
    result = {
        "nodes": [(["Entity"], {"ID": i}) for i in range(5)],
        "relations": []
    }
    iterator = PandasDataFrameIterator(data, "Entity")
    # We run with batchsize 1 to make sure that the serialization is actually used
    converter = Converter(load_file(schema_file_name), iterator, uri, auth, serialize=True, batch_size=1)
    try:
        converter()
    except CancelException:
        pass
    compare(session, result)

def test_raise_when_serialize_and_multiple_workers(session, uri, auth):
    """Tests the exception when using the serialize options as well as specifying multiple workers."""
    data = pd.DataFrame({"ID": list(range(10)), "next": list(range(1,11))})
    iterator = PandasDataFrameIterator(data, "Entity")
    with pytest.raises(ValueError) as excinfo:
        converter = Converter(load_file(schema_file_name), iterator, uri, auth, serialize=True, num_workers=10)
    exception_msg = excinfo.value.args[0]
    assert exception_msg == "You can't use serialization and parallel processing (num_workers > 1) at the same time."
