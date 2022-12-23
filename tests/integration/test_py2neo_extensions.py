#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Integration tests for testing the py2neo extensions and behavior of parallel relations.

authors: Julian Minder
"""

import pytest 

import pandas as pd
from py2neo import Graph

from rel2graph import Converter, IteratorIterator, register_attribute_postprocessor, Attribute
from rel2graph.relational_modules.pandas import PandasDataframeIterator
from rel2graph.py2neo_extensions import GraphWithParallelRelations, MERGE_RELATIONS
import rel2graph.common_modules

def get_relations(graph):
    return graph.relationships.match().all()
    

@pytest.mark.parametrize("config",[(1,True), (1, False) ,(5, False)])
def test_standart(config):
    graph = Graph()
    graph.delete_all()

    schema = """
    ENTITY("Entity"):
        NODE("Entity") node:
            + id = INT(Entity.id)
    
    ENTITY("Relation"):
        RELATION(MATCH("Entity", id = INT(Relation.source_id)), "RELATED_TO", MATCH("Entity", id = INT(Relation.target_id))):
    """
    entities = pd.DataFrame({"id": [1,2]})
    relations = pd.DataFrame({"source_id": [1,1], "target_id": [2,2]})
    iterator = IteratorIterator([PandasDataframeIterator(entities, "Entity"), PandasDataframeIterator(relations, "Relation")])
    converter = Converter(schema, iterator, graph, serialize=config[1], num_workers=config[0])
    converter()
    assert len(get_relations(graph)) == 1

@pytest.mark.parametrize("config",[(1,True), (1, False) ,(5, False)])
def test_standart_same_resource(config):
    graph = Graph()
    graph.delete_all()

    schema = """
    ENTITY("Entity"):
        NODE("Entity") node:
            + id = INT(Entity.id)
    
    ENTITY("Relation"):
        RELATION(MATCH("Entity", id = INT(Relation.source_id)), "RELATED_TO", MATCH("Entity", id = INT(Relation.target_id))):
        RELATION(MATCH("Entity", id = INT(Relation.source_id)), "RELATED_TO", MATCH("Entity", id = INT(Relation.target_id))):
    """
    entities = pd.DataFrame({"id": [1,2]})
    relations = pd.DataFrame({"source_id": [1], "target_id": [2]})
    iterator = IteratorIterator([PandasDataframeIterator(entities, "Entity"), PandasDataframeIterator(relations, "Relation")])
    converter = Converter(schema, iterator, graph, serialize=config[1], num_workers=config[0])
    converter()
    assert len(get_relations(graph)) == 1

@pytest.mark.parametrize("config",[(1,True), (1, False) ,(5, False)])
def test_parallel_relations_different_resource(config):
    graph = GraphWithParallelRelations()
    graph.delete_all()

    schema = """
    ENTITY("Entity"):
        NODE("Entity") node:
            + id = INT(Entity.id)
    
    ENTITY("Relation"):
        RELATION(MATCH("Entity", id = INT(Relation.source_id)), "RELATED_TO", MATCH("Entity", id = INT(Relation.target_id))):
    """
    entities = pd.DataFrame({"id": [1,2]})
    relations = pd.DataFrame({"source_id": [1,1], "target_id": [2,2]})
    iterator = IteratorIterator([PandasDataframeIterator(entities, "Entity"), PandasDataframeIterator(relations, "Relation")])
    converter = Converter(schema, iterator, graph, serialize=config[1], num_workers=config[0])
    converter()
    assert len(get_relations(graph)) == 2

@pytest.mark.parametrize("config",[(1,True), (1, False) ,(5, False)])
def test_parallel_relations_same_resource(config):
    graph = GraphWithParallelRelations()
    graph.delete_all()

    schema = """
    ENTITY("Entity"):
        NODE("Entity") node:
            + id = INT(Entity.id)
    
    ENTITY("Relation"):
        RELATION(MATCH("Entity", id = INT(Relation.source_id)), "RELATED_TO", MATCH("Entity", id = INT(Relation.target_id))):
        RELATION(MATCH("Entity", id = INT(Relation.source_id)), "RELATED_TO", MATCH("Entity", id = INT(Relation.target_id))):
    """
    entities = pd.DataFrame({"id": [1,2]})
    relations = pd.DataFrame({"source_id": [1], "target_id": [2]})
    iterator = IteratorIterator([PandasDataframeIterator(entities, "Entity"), PandasDataframeIterator(relations, "Relation")])
    converter = Converter(schema, iterator, graph, serialize=config[1], num_workers=config[0])
    converter()
    assert len(get_relations(graph)) == 2

@pytest.mark.parametrize("config",[(1,True), (1, False) ,(5, False)])
def test_merge_relations(config):
    graph = GraphWithParallelRelations()
    graph.delete_all()

    schema = """
    ENTITY("Entity"):
        NODE("Entity") node:
            + id = INT(Entity.id)
    
    ENTITY("Relation"):
        MERGE_RELATIONS(RELATION(MATCH("Entity", id = INT(Relation.source_id)), "RELATED_TO", MATCH("Entity", id = INT(Relation.target_id)))):
    """
    entities = pd.DataFrame({"id": [1,2]})
    relations = pd.DataFrame({"source_id": [1,1], "target_id": [2,2]})
    iterator = IteratorIterator([PandasDataframeIterator(entities, "Entity"), PandasDataframeIterator(relations, "Relation")])
    converter = Converter(schema, iterator, graph, serialize=config[1], num_workers=config[0])
    converter()
    assert len(get_relations(graph)) == 1
