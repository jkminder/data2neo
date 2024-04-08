#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tests for create and merge functionality.

authors: Julian Minder
"""
import os
import pytest
import datetime
from neo4j import GraphDatabase, time

from rel2graph.neo4j import Node, Relationship, Subgraph, create, merge, match_nodes, match_relationships
from rel2graph.common_modules import MERGE_RELATIONSHIPS

@pytest.fixture
def session():
    # Check if custom port
    try:
        port = os.environ["NEO4J PORT"]
    except KeyError:
        port = 7687
    # Initialise graph
    
    driver = GraphDatabase.driver("bolt://localhost:{}".format(port), auth=("neo4j", "password"))
    with driver.session() as session:
        try:    
            delete_all(session)
            # generate test data
            n1 = Node("test", "second", id=1, name="test1", anotherattr="test")
            n2 = Node("test", id=2, name="test2", anotherattr="test")
            n3 = Node("anotherlabel", id=3, name="test3")

            r1 = Relationship(n1, "to", n2, id=1)
            r2 = Relationship(n1, "to", n3, id=2, anotherattr="test")

            graph = n1 | n2 | n3 | r1 | r2
            create(graph, session)
            yield session
        finally:
            session.close()
            driver.close()
            return
        
def check_node(nodes, id):
    return len([node for node in nodes if node["id"] == id]) == 1

def check_rel(rels, id):
    return len([rel for rel in rels if rel["id"] == id]) == 1

def delete_all(session):
    session.run("MATCH (n) DETACH DELETE n")

def test_match_nodes(session):
    # match by single label
    nodes = match_nodes(session, "test")
    assert(len(nodes) == 2)
    assert(check_node(nodes, 1))
    assert(check_node(nodes, 2))

    # match by multiple labels
    nodes = match_nodes(session, "test", "second")
    assert(len(nodes) == 1)
    assert(check_node(nodes, 1))

    # match by properties with no label
    nodes = match_nodes(session, name="test3")
    assert(len(nodes) == 1)
    assert(check_node(nodes, 3))

    # match by properties with label
    nodes = match_nodes(session, "test", name="test1")
    assert(len(nodes) == 1)
    assert(check_node(nodes, 1))

    # match by two properties
    nodes = match_nodes(session, name="test1", anotherattr="test")
    assert(len(nodes) == 1)
    assert(check_node(nodes, 1))

def test_match_relationships(session):
    # match by type
    rels = match_relationships(session, rel_type="to")
    assert(len(rels) == 2)
    assert(check_rel(rels, 1))
    assert(check_rel(rels, 2))

    # match by properties
    rels = match_relationships(session, rel_type="to", id=1)
    assert(len(rels) == 1)
    assert(check_rel(rels, 1))  

    # match by multiple properties
    rels = match_relationships(session, rel_type="to", id=2, anotherattr="test")
    assert(len(rels) == 1)
    assert(check_rel(rels,2))

    # match by from node
    n1 = match_nodes(session, "test", id=1)[0]
    rels = match_relationships(session, from_node=n1)
    assert(len(rels) == 2)
    assert(check_rel(rels, 1))
    assert(check_rel(rels, 2))

    # match by to node
    n2 = match_nodes(session, "test", id=2)[0]
    rels = match_relationships(session, to_node=n2)
    assert(len(rels) == 1)
    assert(check_rel(rels, 1))

