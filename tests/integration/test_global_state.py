#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Integration tests for global state in rel2graph

authors: Julian Minder
"""

import pytest 

from py2neo import Graph
import pandas as pd
import multiprocessing as mp

from rel2graph import Converter, GlobalSharedState, register_subgraph_postprocessor
from rel2graph.relational_modules.pandas import PandasDataframeIterator

@register_subgraph_postprocessor
def CHECK_GRAPH(subgraph):
    # throws an error if the graph is not connected
    GlobalSharedState.graph.run("MATCH (n) RETURN n")
    return subgraph

@register_subgraph_postprocessor
def CHECK_STATIC_VARIABLE(subgraph):
    assert GlobalSharedState.static_variable == 1
    return subgraph

@register_subgraph_postprocessor
def SET_SHARED_VARIABLE(subgraph):
    GlobalSharedState.static_variable.value = 2
    return subgraph

@register_subgraph_postprocessor
def UPDATE_SHARED_VARIABLE(subgraph):
    with GlobalSharedState.lock:
        GlobalSharedState.static_variable.value += 1
    return subgraph

@register_subgraph_postprocessor
def ADD_ELEMENT_TO_MANAGER(subgraph):
    GlobalSharedState.list_of_python_obj.append(1)
    return subgraph

def test_get_graph():
    graph = Graph()
    graph.delete_all()

    schema = """
    ENTITY("Entity"):
        CHECK_GRAPH(NODE("Entity")) node:
            + id = INT(Entity.id)   
    """

    entities = pd.DataFrame({"id": [1,2]})
    iterator = PandasDataframeIterator(entities, "Entity")
    converter = Converter(schema, iterator, graph, num_workers=2, batch_size=1)
    converter()
    assert len(graph.nodes.match().all()) == 2

def test_raise_set_illegal_attribute():
    with pytest.raises(AttributeError):
        GlobalSharedState.graph = 1
    with pytest.raises(AttributeError):
        GlobalSharedState._custom_global_vars = 1

def test_static_variable():
    graph = Graph()
    graph.delete_all()

    schema = """
    ENTITY("Entity"):
        CHECK_STATIC_VARIABLE(NODE("Entity")) node:
            + id = INT(Entity.id)   
    """

    entities = pd.DataFrame({"id": [1,2]})
    iterator = PandasDataframeIterator(entities, "Entity")
    GlobalSharedState.static_variable = 1
    converter = Converter(schema, iterator, graph, num_workers=2, batch_size=1)
    converter()
    assert GlobalSharedState.static_variable == 1

def test_set_variable():
    graph = Graph()
    graph.delete_all()

    schema = """
    ENTITY("Entity"):
        SET_SHARED_VARIABLE(NODE("Entity")) node:
            + id = INT(Entity.id)   
    """

    entities = pd.DataFrame({"id": [1,2]})
    iterator = PandasDataframeIterator(entities, "Entity")
    GlobalSharedState.static_variable = mp.Value('i', 1)
    converter = Converter(schema, iterator, graph, num_workers=2, batch_size=1)
    converter()
    assert GlobalSharedState.static_variable.value == 2

def test_update_variable():
    graph = Graph()
    graph.delete_all()

    schema = """
    ENTITY("Entity"):
        UPDATE_SHARED_VARIABLE(NODE("Entity")) node:
            + id = INT(Entity.id)   
    """

    entities = pd.DataFrame({"id": [1,2]})
    iterator = PandasDataframeIterator(entities, "Entity")
    GlobalSharedState.static_variable = mp.Value('i', 1)
    GlobalSharedState.lock = mp.Lock()
    converter = Converter(schema, iterator, graph, num_workers=2, batch_size=1)
    converter()
    assert GlobalSharedState.static_variable.value == 3

def test_manager():
    graph = Graph()
    graph.delete_all()

    schema = """
    ENTITY("Entity"):
        ADD_ELEMENT_TO_MANAGER(NODE("Entity")) node:
            + id = INT(Entity.id)   
    """

    entities = pd.DataFrame({"id": [1,2]})
    iterator = PandasDataframeIterator(entities, "Entity")
    manager = mp.Manager()
    GlobalSharedState.list_of_python_obj = manager.list()
    converter = Converter(schema, iterator, graph, num_workers=2, batch_size=1)
    converter()
    assert GlobalSharedState.list_of_python_obj[:] == [1,1]