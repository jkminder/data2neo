#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Integration tests for testing the MERGE_RELATIONSHIPS wrapper.

authors: Julian Minder
"""

import pytest 

import pandas as pd

from rel2graph import Converter, IteratorIterator, register_attribute_postprocessor, Attribute
from rel2graph.relational_modules.pandas import PandasDataFrameIterator
import rel2graph.common_modules.types
from rel2graph.common_modules import MERGE_RELATIONS

from helpers import *



@pytest.mark.parametrize("config",[(1,True), (1, False) ,(5, False)])
def test_standart(config, session, uri, auth):

    schema = """
    ENTITY("Entity"):
        NODE("Entity") node:
            + id = INT(Entity.id)
    
    ENTITY("Relation"):
        RELATION(MATCH("Entity", id = INT(Relation.source_id)), "RELATED_TO", MATCH("Entity", id = INT(Relation.target_id))):
    """
    entities = pd.DataFrame({"id": [1,2]})
    relations = pd.DataFrame({"source_id": [1,1], "target_id": [2,2]})
    iterator = IteratorIterator([PandasDataFrameIterator(entities, "Entity"), PandasDataFrameIterator(relations, "Relation")])
    converter = Converter(schema, iterator, uri, auth, serialize=config[1], num_workers=config[0])
    converter()
    assert num_relationships(session) == 2

@pytest.mark.parametrize("config",[(1,True), (1, False) ,(5, False)])
def test_standart_same_resource(config, session, uri, auth):
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
    iterator = IteratorIterator([PandasDataFrameIterator(entities, "Entity"), PandasDataFrameIterator(relations, "Relation")])
    converter = Converter(schema, iterator, uri, auth, serialize=config[1], num_workers=config[0])
    converter()
    assert num_relationships(session) == 2


@pytest.mark.parametrize("config",[(1,True), (1, False) ,(5, False)])
def test_merge(config, session, uri, auth):
    schema = """
    ENTITY("Entity"):
        NODE("Entity") node:
            + id = INT(Entity.id)
    
    ENTITY("Relation"):
        MERGE_RELATIONSHIPS(RELATION(MATCH("Entity", id = INT(Relation.source_id)), "RELATED_TO", MATCH("Entity", id = INT(Relation.target_id)))):
    """
    entities = pd.DataFrame({"id": [1,2]})
    relations = pd.DataFrame({"source_id": [1,1], "target_id": [2,2]})
    iterator = IteratorIterator([PandasDataFrameIterator(entities, "Entity"), PandasDataFrameIterator(relations, "Relation")])
    converter = Converter(schema, iterator, uri, auth, serialize=config[1], num_workers=config[0])
    converter()
    assert num_relationships(session) == 1

@pytest.mark.parametrize("config",[(1,True), (1, False) ,(5, False)])
def test_merge_same_resource(config, session, uri, auth):
    schema = """
    ENTITY("Entity"):
        NODE("Entity") node:
            + id = INT(Entity.id)
    
    ENTITY("Relation"):
        MERGE_RELATIONSHIPS(RELATION(MATCH("Entity", id = INT(Relation.source_id)), "RELATED_TO", MATCH("Entity", id = INT(Relation.target_id)))):
        MERGE_RELATIONSHIPS(RELATION(MATCH("Entity", id = INT(Relation.source_id)), "RELATED_TO", MATCH("Entity", id = INT(Relation.target_id)))):
    """
    entities = pd.DataFrame({"id": [1,2]})
    relations = pd.DataFrame({"source_id": [1], "target_id": [2]})
    iterator = IteratorIterator([PandasDataFrameIterator(entities, "Entity"), PandasDataFrameIterator(relations, "Relation")])
    converter = Converter(schema, iterator, uri, auth, serialize=config[1], num_workers=config[0])
    converter()
    assert num_relationships(session) == 1