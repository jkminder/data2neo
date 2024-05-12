#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Integration tests for testing the merging and MERGE_RELATIONSHIPS wrapper.

authors: Julian Minder
"""

import pytest 

import pandas as pd

from data2neo import Converter, IteratorIterator, register_attribute_postprocessor, Attribute
from data2neo.relational_modules.pandas import PandasDataFrameIterator
import data2neo.common_modules.types
from data2neo.common_modules import MERGE_RELATIONS

from helpers import *



@pytest.mark.parametrize("config",[(1,True), (1, False) ,(5, False)])
def test_standart(config, session, uri, auth):

    schema = """
    ENTITY("Entity"):
        NODE("Entity") node:
            + id = INT(Entity.id)
    
    ENTITY("Relationship"):
        RELATIONSHIP(MATCH("Entity", id = INT(Relation.source_id)), "RELATED_TO", MATCH("Entity", id = INT(Relation.target_id))):
    """
    entities = pd.DataFrame({"id": [1,2]})
    relations = pd.DataFrame({"source_id": [1,1], "target_id": [2,2]})
    iterator = IteratorIterator([PandasDataFrameIterator(entities, "Entity"), PandasDataFrameIterator(relations, "Relationship")])
    converter = Converter(schema, iterator, uri, auth, serialize=config[1], num_workers=config[0])
    converter()
    assert num_relationships(session) == 2

@pytest.mark.parametrize("config",[(1,True), (1, False) ,(5, False)])
def test_standart_same_resource(config, session, uri, auth):
    schema = """
    ENTITY("Entity"):
        NODE("Entity") node:
            + id = INT(Entity.id)
    
    ENTITY("Relationship"):
        RELATIONSHIP(MATCH("Entity", id = INT(Relation.source_id)), "RELATED_TO", MATCH("Entity", id = INT(Relation.target_id))):
        RELATIONSHIP(MATCH("Entity", id = INT(Relation.source_id)), "RELATED_TO", MATCH("Entity", id = INT(Relation.target_id))):
    """
    entities = pd.DataFrame({"id": [1,2]})
    relations = pd.DataFrame({"source_id": [1], "target_id": [2]})
    iterator = IteratorIterator([PandasDataFrameIterator(entities, "Entity"), PandasDataFrameIterator(relations, "Relationship")])
    converter = Converter(schema, iterator, uri, auth, serialize=config[1], num_workers=config[0])
    converter()
    assert num_relationships(session) == 2


@pytest.mark.parametrize("config",[(1,True), (1, False) ,(5, False)])
def test_merge_nodes(config, session, uri, auth):
    schema = """
    ENTITY("Entity"):
        NODE("Entity") node:
            + id = INT(Entity.id)
    """
    entities = pd.DataFrame({"id": [1,2, 1, 2]})
    iterator = IteratorIterator([PandasDataFrameIterator(entities, "Entity")])
    converter = Converter(schema, iterator, uri, auth, serialize=config[1], num_workers=config[0])    
    converter()
    assert num_nodes(session) == 2

@pytest.mark.parametrize("config",[(1,True), (1, False) ,(5, False)])
def test_merge_relationships(config, session, uri, auth):
    schema = """
    ENTITY("Entity"):
        NODE("Entity") node:
            + id = INT(Entity.id)
    
    ENTITY("Relation"):
        MERGE_RELATIONSHIPS(RELATIONSHIP(MATCH("Entity", id = INT(Relation.source_id)), "RELATED_TO", MATCH("Entity", id = INT(Relation.target_id)))):
    """
    entities = pd.DataFrame({"id": [1,2]})
    relations = pd.DataFrame({"source_id": [1,1], "target_id": [2,2]})
    iterator = IteratorIterator([PandasDataFrameIterator(entities, "Entity"), PandasDataFrameIterator(relations, "Relation")])
    converter = Converter(schema, iterator, uri, auth, serialize=config[1], num_workers=config[0], batch_size=1)    
    converter()
    assert num_relationships(session) == 1

@pytest.mark.parametrize("config",[(1,True)])
def test_merge_relationships_same_resource(config, session, uri, auth):
    schema = """
    ENTITY("Entity"):
        NODE("Entity") node:
            + id = INT(Entity.id)
    
    ENTITY("Relation"):
        MERGE_RELATIONSHIPS(RELATIONSHIP(MATCH("Entity", id = INT(Relation.source_id)), "RELATED_TO", MATCH("Entity", id = INT(Relation.target_id)))):
        MERGE_RELATIONSHIPS(RELATIONSHIP(MATCH("Entity", id = INT(Relation.source_id)), "RELATED_TO", MATCH("Entity", id = INT(Relation.target_id)))):
    """
    entities = pd.DataFrame({"id": [1,2]})
    relations = pd.DataFrame({"source_id": [1], "target_id": [2]})
    iterator = IteratorIterator([PandasDataFrameIterator(entities, "Entity"), PandasDataFrameIterator(relations, "Relation")])
    converter = Converter(schema, iterator, uri, auth, serialize=config[1], num_workers=config[0], batch_size=1)
    converter()
    n = num_relationships(session)
    if n != 1:
        with session.begin_transaction() as tx:
            for record in tx.run("MATCH (a) RETURN a.id, id(a)"):
                print(record)
            for record in tx.run("MATCH (a)-[r]->(b) RETURN r, a.id, b.id, id(r), id(a), id(b)"):
                print(record)
        print("n", n)
        exit()
    assert num_relationships(session) == 1