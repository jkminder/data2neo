#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tests for py2neo extensions module.

authors: Julian Minder
"""
from py2neo.errors import ConnectionUnavailable
import os
import pytest
from rel2graph.py2neo_extensions import GraphWithParallelRelations, MERGE_RELATIONS
from py2neo import Relationship, Node

@pytest.fixture
def graphwpr():
    # Check if custom port
    try:
        port = os.environ["NEO4J PORT"]
    except KeyError:
        port = 7687
    # Initialise graph
    try:
        graphwpr = GraphWithParallelRelations(scheme="bolt", host="localhost", port=port)
    except ConnectionUnavailable:
        raise ConnectionUnavailable("These tests need a running local instance of neo4j...")
    graphwpr.delete_all()
    yield graphwpr
    graphwpr.delete_all()

def get_nodes(graph):
    return graph.run("MATCH (c:test) RETURN c").to_subgraph()

def get_relations(graph):
    return graph.run("MATCH p=()-->() RETURN p").to_subgraph()

def test_create_nodes(graphwpr):
    n1 = Node("test", id=1)
    n2 = Node("test", id=2)
    n3 = Node("test", id=2)
    graphwpr.create(n1 | n2 | n3)
    res = get_nodes(graphwpr)
    assert(len(res.nodes)==3)
    assert(len([node for node in res.nodes if node["id"] == 2 and "test" in node.labels]) == 2)
    assert(len([node for node in res.nodes if node["id"] == 1 and "test" in node.labels]) == 1)

def test_create_relations(graphwpr):
    n1 = Node("test", id=1)
    n2 = Node("test", id=2)
    r1 = Relationship(n1, "to", n2)
    graphwpr.create(r1)
    rels = get_relations(graphwpr).relationships
    assert(len(rels) == 1)
    assert(type(rels[0]).__name__ == "to")

def test_create_parallel_relations_with_id(graphwpr):
    n1 = Node("test", id=1)
    n2 = Node("test", id=2)
    r1 = Relationship(n1, "to", n2, id=1)
    r2 = Relationship(n1, "to", n2, id=2)
    r3 = Relationship(n1, "to", n2, id=3)
    r4 = Relationship(n1, "relates to", n2)

    graphwpr.create(r1|r2|r3|r4) # this results in a subgraph with 4 relations (bc of different attribtues)
    rels = get_relations(graphwpr).relationships
    assert(len(rels) == 4)
    assert(len([r for r in rels if r.start_node == n1 and r.end_node == n2 and type(r).__name__ == "to"]) == 3)
    assert(len([r for r in rels if r.start_node == n1 and r.end_node == n2]) == 4)

def test_create_parallel_relations_no_id(graphwpr):
    # More a test for subgraph behavior
    n1 = Node("test", id=1)
    n2 = Node("test", id=2)
    r1 = Relationship(n1, "to", n2)
    r2 = Relationship(n1, "to", n2)
    r3 = Relationship(n1, "to", n2)
    r4 = Relationship(n1, "relates to", n2)

    graphwpr.create(r1|r2|r3|r4)
    rels = get_relations(graphwpr).relationships
    assert(len(rels) == 2)
    assert(len([r for r in rels if r.start_node == n1 and r.end_node == n2 and type(r).__name__ == "to"]) == 1)
    assert(len([r for r in rels if r.start_node == n1 and r.end_node == n2]) == 2)

def test_merge_nodes_no_pk(graphwpr):
    n1 = Node("test", id=1)
    n2 = Node("test", id=2)
    n3 = Node("test", id=2)
    
    with pytest.raises(ValueError): # without primary keys
        graphwpr.merge(n1 | n2 | n3)

def test_merge_nodes_pk_arg(graphwpr):
    n1 = Node("test", id=1)
    n2 = Node("test", id=2)
    n3 = Node("test", id=2)
    
    graphwpr.merge(n1 | n2 | n3, "test", "id")
    nodes = get_nodes(graphwpr).nodes
    assert(len(nodes)==2)
    assert(len([node for node in nodes if node["id"] == 2 and "test" in node.labels]) == 1)
    assert(len([node for node in nodes if node["id"] == 1 and "test" in node.labels]) == 1)

def test_merge_nodes_pk_in_node(graphwpr):
    n1 = Node("test", id=1)
    n2 = Node("test", id=2)
    n3 = Node("test", id=2)
    n4 = Node("test", id=1)
    for n in (n1,n2,n3,n4):
        n.__primarykey__ = "id"
        n.__primarylabel__ = "test"
    
    graphwpr.merge(n1 | n2 | n3)
    nodes = get_nodes(graphwpr).nodes
    assert(len(nodes)==2)
    assert(len([node for node in nodes if node["id"] == 2 and "test" in node.labels]) == 1)
    assert(len([node for node in nodes if node["id"] == 1 and "test" in node.labels]) == 1)
    
    graphwpr.merge(n4)
    assert(len(nodes)==2)
    assert(len([node for node in nodes if node["id"] == 2 and "test" in node.labels]) == 1)
    assert(len([node for node in nodes if node["id"] == 1 and "test" in node.labels]) == 1)

def test_merge_relations_no_pk(graphwpr):
    n1 = Node("test", id=1)
    n2 = Node("test", id=2)
    r1 = Relationship(n1, "to", n2, id=1)
    graphwpr.create(n1|n2|r1)
    r2 = Relationship(n1, "to", n2, id=1)
    with pytest.raises(ValueError): # without primary keys
        graphwpr.merge(r2)

def test_merge_relations_pk_arg(graphwpr):
    n1 = Node("test", id=1)
    n2 = Node("test", id=2)
    r1 = Relationship(n1, "to", n2, id=1)
    graphwpr.create(n1|n2|r1)
    r2 = Relationship(n1, "to", n2, id=1)
    graphwpr.merge(r2, None, "id")
    rels = get_relations(graphwpr).relationships
    assert(len(rels) == 1)
    assert(type(rels[0]).__name__ == "to")

def test_merge_relations_pk_in_rel(graphwpr):
    n1 = Node("test", id=1)
    n2 = Node("test", id=2)
    r1 = Relationship(n1, "to", n2, id=1)
    r2 = Relationship(n1, "to", n2, id=2)
    graphwpr.create(n1|n2|r1|r2)
    r3 = Relationship(n1, "to", n2, id=1)
    r3.__primarykey__ = "id"
    r4 = Relationship(n1, "to", n2, id=2)
    r4.__primarykey__ = "id"
    graphwpr.merge(r3|r4)
    rels = get_relations(graphwpr).relationships
    assert(len(rels) == 2)
    assert(len([r for r in rels if r.start_node == n1 and r.end_node == n2 and type(r).__name__ == "to"]) == 2)
    assert(len([r for r in rels if r["id"]==1]) == 1)
    assert(len([r for r in rels if r["id"]==2]) == 1)

def test_merge_relationation_no_pk(graphwpr):
    n1 = Node("test", id=1)
    n2 = Node("test", id=2)
    r1 = Relationship(n1, "to", n2)
    graphwpr.create(n1|n2|r1)

    r2 = MERGE_RELATIONS(Relationship(n1, "to", n2))
    graphwpr.merge(r2)
    rels = get_relations(graphwpr).relationships
    assert(len(rels) == 1)
    assert(len([r for r in rels if r.start_node == n1 and r.end_node == n2 and type(r).__name__ == "to"]) == 1)
