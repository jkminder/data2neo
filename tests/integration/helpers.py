#!/usr/bin/env python
# -*- coding: utf-8 -*-

import neo4j
import os 
import pytest

from rel2graph import register_subgraph_postprocessor
from rel2graph.neo4j import Node, Relationship, Subgraph

import warnings

@pytest.fixture
def session():
    # This will be raised by neo4j due to how the session is yielded
    # You can ignore this as it is correctly closed
    warnings.filterwarnings("ignore", category=DeprecationWarning) 
    
    # Check if custom port
    try:
        port = os.environ["NEO4J PORT"]
    except KeyError:
        port = 7687
    # Initialise graph
    driver = neo4j.GraphDatabase.driver("bolt://localhost:{}".format(port), auth=("neo4j", "password"))
    with driver.session() as session:
        try:    
            delete_all(session)
            yield session
        finally:
            pass        
        
@pytest.fixture
def uri():
    # Check if custom port
    try:
        port = os.environ["NEO4J PORT"]
    except KeyError:
        port = 7687
    return "bolt:localhost:{}".format(port)

@pytest.fixture
def auth():
    return neo4j.basic_auth("neo4j", "password")

def delete_all(session):
    session.run("MATCH (n) DETACH DELETE n;")

def num_nodes(session):
    return session.run("MATCH (n) RETURN COUNT(DISTINCT n) AS num_nodes").single()["num_nodes"]

def num_relationships(session):
    return session.run("MATCH ()-[r]->() RETURN COUNT(DISTINCT r) AS num_relations").single()["num_relations"]

def get_nodes(session, labels=[]):
    if not isinstance(labels, list) and not isinstance(labels, tuple):
        labels = [labels]
    # TODO: id() is deprecated, in the future we need to move to something else
    res = session.run("MATCH (n{}) RETURN LABELS(n) as labels, n as properties, id(n) as identity".format(":" + ":".join(labels) if len(labels) else "")).data()
    match_list = [Node.from_dict(r['labels'], r['properties'], identity=r["identity"]) for r in res]
    return match_list

def get_relationships(session, types=[]):
    if not isinstance(types, list) and not isinstance(types, tuple):
        types = [types]
    # TODO: id() is deprecated, in the future we need to move to something else
    res = session.run("""MATCH (a)-[r{}]->(b) RETURN TYPE(r) as type, PROPERTIES(r) as properties, id(r) as identity, 
                        LABELS(a) as start_labels, a as start_properties, id(a) as start,
                        LABELS(b) as end_labels, b as end_properties, id(b) as end
                      """.format(":" + "|".join(types) if len(types) else "")).data()
    
    match_list = [Relationship(Node.from_dict(r['start_labels'], r['start_properties'], identity=r["start"]),
                               r["type"],
                               Node.from_dict(r['end_labels'], r['end_properties'], identity=r["end"]),
                               **r["properties"]) for r in res]
    
    return match_list

def eq_node(rnode, gnode):
    # same labels
    if set(rnode[0]) != gnode.labels:
        return False
    # same number of properties
    if len(rnode[1]) != len(gnode):
        return False
    # same properties
    for (key, value) in rnode[1].items():
        if gnode[key] != value:
            return False
    return True

def eq_relationship(rrel, grel):
    # same type
    if rrel[1] != grel.type:
        return False
    # same from and to
    if not eq_node(rrel[0], grel.start_node) or not eq_node(rrel[2], grel.end_node):
        return False
    # same number of properties
    if len(rrel[3]) != len(grel):
        return False
    # same properties
    for (key, value) in rrel[3].items():
        if grel[key] != value:
            return False
    return True
    
def compare_nodes(session, result):
    graphnodes = get_nodes(session)
    print("Graph Nodes: ", graphnodes)
    print("Result Nodes: ", result["nodes"])
    assert len(graphnodes) == len(result["nodes"]), "Same number of nodes"
    for rnode in result["nodes"]:
        found = False
        for gnode in graphnodes:
            found = found or eq_node(rnode, gnode)
            if found:
                break
        assert found, f"The following node was not found: {rnode}"

def compare_relationships(session, result):
    graph_relationships = get_relationships(session)
    print("Graph Relationships: ")
    for grel in graph_relationships:
        print("- ", grel)
    print("Result Relationships: ")
    for rrel in result["relationships"]:
        print("- ", rrel)
    assert len(graph_relationships) == len(result["relationships"]), "Same number of relationship"
    for rrel in result["relationships"]:
        found = False
        for grel in graph_relationships:
            found = found or eq_relationship(rrel, grel)
            if found:
                break
        assert found, f"The following relationship was not found: {rrel}"

def compare(session, result):
    compare_nodes(session, result)
    compare_relationships(session, result)