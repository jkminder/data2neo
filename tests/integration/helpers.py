#!/usr/bin/env python
# -*- coding: utf-8 -*-

import neo4j
import os 
import pytest

from rel2graph import register_subgraph_postprocessor
from rel2graph.neo4j import Node, Relationship, Subgraph


@pytest.fixture
def session():
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
    session.run("MATCH (n) DETACH DELETE n")

def num_nodes(session):
    return session.run("MATCH (n) RETURN COUNT(n) AS num_nodes").single()["num_nodes"]

def num_relationships(session):
    return session.run("MATCH ()-[r]->() RETURN COUNT(r) AS num_relations").single()["num_relations"]

def get_nodes(session, labels=[]):
    if not isinstance(labels, list) and not isinstance(labels, tuple):
        labels = [labels]
    res = session.run("MATCH (n{}) RETURN LABELS(n) as labels, n as properties, elementId(n) as identity".format(":" + ":".join(labels) if len(labels) else "")).data()
    match_list = [Node.from_dict(r['labels'], r['properties'], identity=r["identity"]) for r in res]
    return match_list

def get_relations(session, types=[]):
    if not isinstance(types, list) and not isinstance(types, tuple):
        types = [types]
    res = session.run("""MATCH (a)-[r{}]->(b) RETURN TYPE(r) as type, PROPERTIES(r) as properties, elementId(r) as identity, 
                        LABELS(a) as start_labels, a as start_properties, elementId(a) as start,
                        LABELS(b) as end_labels, b as end_properties, elementId(b) as end
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

def eq_relation(rrel, grel):
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

def compare_relations(session, result):
    graph_relations = get_relations(session)
    print("Graph Relations: ")
    for grel in graph_relations:
        print("- ", grel)
    print("Result Relations: ")
    for rrel in result["relations"]:
        print("- ", rrel)
    assert len(graph_relations) == len(result["relations"]), "Same number of relations"
    for rrel in result["relations"]:
        found = False
        for grel in graph_relations:
            found = found or eq_relation(rrel, grel)
            if found:
                break
        assert found, f"The following relation was not found: {rrel}"

def compare(session, result):
    compare_nodes(session, result)
    compare_relations(session, result)