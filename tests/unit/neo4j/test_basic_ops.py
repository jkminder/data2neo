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

from rel2graph.neo4j import Node, Relationship, Subgraph, create, merge, push, pull
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
            yield session
        finally:
            session.close()
            driver.close()
            return
        
def get_nodes(session):
    return session.run("MATCH (c:test) RETURN c, labels(c)").data()

def get_relationships(session):
    return session.run("MATCH p=()-[d]->() RETURN p, properties(d) AS props").data()


def delete_all(session):
    session.run("MATCH (n) DETACH DELETE n")

def test_create_nodes(session):
    # create single node
    n1 = Node("test", id=1)
    create(n1, session)

    nodes = get_nodes(session)
    assert(len(nodes) == 1)
    assert(nodes[0]["c"]["id"] == 1)

    # create multiple nodes
    n2 = Node("test", id=2)
    n3 = Node("test", id=3)
    graph = n2 | n3
    create(graph, session)

    nodes = get_nodes(session)
    assert(len(nodes) == 3)
    assert(len([node for node in nodes if node["c"]["id"] == 1]) == 1)
    assert(len([node for node in nodes if node["c"]["id"] == 2]) == 1)
    assert(len([node for node in nodes if node["c"]["id"] == 3]) == 1)

def test_create_relations(session):
    n1 = Node("test", id=1)
    n2 = Node("test", id=2)
    n3 = Node("test", id=3)
    r1 = Relationship(n1, "to", n2)
    create(r1, session)

    rels = get_relationships(session)
    assert(len(rels) == 1)
    assert rels[0]["p"][0]["id"] == 1
    assert rels[0]["p"][2]["id"] == 2
    assert(rels[0]["p"][1] == "to") 

    # create multiple relations
    r2 = Relationship(n1, "another", n2)
    r3 = Relationship(n1, "to", n3)

    graph = r2 | r3
    create(graph, session)

    rels = get_relationships(session)
    assert(len(rels) == 3)
    assert(len([rel for rel in rels if rel["p"][0]["id"] == 1 and rel["p"][2]["id"] == 2 and rel["p"][1] == "to"]) == 1)
    assert(len([rel for rel in rels if rel["p"][0]["id"] == 1 and rel["p"][2]["id"] == 2 and rel["p"][1] == "another"]) == 1)
    assert(len([rel for rel in rels if rel["p"][0]["id"] == 1 and rel["p"][2]["id"] == 3 and rel["p"][1] == "to"]) == 1)

    # create relation with attributes
    delete_all(session)
    n1 = Node("test", id=1)
    n2 = Node("test", id=2)
    r4 = Relationship(n1, "attribute", n2, attribute=1, another_attribute="test")
    create(r4, session)

    rels = get_relationships(session)
    assert(len(rels) == 1)
    assert rels[0]["p"][0]["id"] == 1
    assert rels[0]["p"][2]["id"] == 2
    assert(rels[0]["p"][1] == "attribute")
    assert(rels[0]["props"]["attribute"] == 1)
    assert(rels[0]["props"]["another_attribute"] == "test")

def test_merge_nodes(session):
    n1 = Node("test", id=1)
    n2 = Node("test", id=2)
    n3 = Node("test", id=2)
    
    create((n1 | n2 | n3), session)

    merge(n1, session, primary_label="test", primary_key="id")
    merge(n2, session, primary_label="test", primary_key="id")
    merge(n3, session, primary_label="test", primary_key="id")
    assert(len(get_nodes(session))==3)

    delete_all(session)
    n1 = Node("test", id=1)
    n2 = Node("test", id=2)
    n3 = Node("test", id=2)
    n1.set_primary_key("id")
    n2.set_primary_key("id")
    n3.set_primary_key("id")
    
    merge((n1 | n2 | n3), session)

    merge(n1, session, primary_label="test", primary_key="id")
    merge(n2, session, primary_label="test", primary_key="id")
    merge(n3, session, primary_label="test", primary_key="id")

    nodes = get_nodes(session)
    assert(len(nodes)==2)
    assert(len([node for node in nodes if node["c"]["id"] == 2]) == 1)
    assert(len([node for node in nodes if node["c"]["id"] == 1]) == 1)

def test_merge_relationships(session):
    n1 = Node("test", id=1)
    n2 = Node("test", id=2)
    r1 = Relationship(n1, "to", n2, pk=1)
    r1.set_primary_key("pk")
    create(r1, session)

    r2 = Relationship(n1, "to", n2, pk=1)
    r2.set_primary_key("pk")
    merge(r2, session)

    rels = get_relationships(session)
    assert(len(rels) == 1)
    assert rels[0]["p"][0]["id"] == 1
    assert rels[0]["p"][2]["id"] == 2
    assert(rels[0]["p"][1] == "to") 

    # create multiple relations
    delete_all(session)
    n1 = Node("test", id=1)
    n2 = Node("test", id=2)
    r3 = Relationship(n1, "another", n2, id=2)
    r3.set_primary_key("id")

    r4 = Relationship(n1, "to", n2, id=2)
    r4.set_primary_key("id")

    graph = r3 | r4
    create(graph, session)

    r5 = Relationship(n1, "another", n2, id=2)
    r5.set_primary_key("id")

    r6 = Relationship(n1, "to", n2, id=2)
    r6.set_primary_key("id")

    graph = r5 | r6

    merge(graph, session)

    rels = get_relationships(session)
    assert(len(rels) == 2)
    assert(len([rel for rel in rels if rel["p"][0]["id"] == 1 and rel["p"][2]["id"] == 2 and rel["p"][1] == "to"]) == 1)
    assert(len([rel for rel in rels if rel["p"][0]["id"] == 1 and rel["p"][2]["id"] == 2 and rel["p"][1] == "another"]) == 1)


def test_create_parallel_relations_with_id(session):
    n1 = Node("test", id=1)
    n2 = Node("test", id=2)
    r1 = Relationship(n1, "to", n2, id=1)
    r2 = Relationship(n1, "to", n2, id=2)
    r3 = Relationship(n1, "to", n2, id=3)
    r4 = Relationship(n1, "relates to", n2)

    graph = r1 | r2 | r3 | r4
    
    create(graph, session) # this results in a subgraph with 4 relations (bc of different attribtues)
    rels = get_relationships(session)
    assert(len(rels) == 4)
    assert(len([rel for rel in rels if rel["p"][0]["id"] == 1 and rel["p"][2]["id"] == 2 and rel["p"][1] == "to"]) == 3)
    assert(len([rel for rel in rels if rel["p"][0]["id"] == 1 and rel["p"][2]["id"] == 2 and rel["p"][1] == "relates to"]) == 1)

def test_create_parallel_relations_no_id(session):
    # More a test for subgraph behavior
    n1 = Node("test", id=1)
    n2 = Node("test", id=2)
    r1 = Relationship(n1, "to", n2)
    r2 = Relationship(n1, "to", n2)
    r3 = Relationship(n1, "to", n2)
    r4 = Relationship(n1, "relates to", n2)

    graph = r1 | r2 | r3 | r4
    
    create(graph, session) # this results in a subgraph with 4 relations (bc of different attribtues)
    rels = get_relationships(session)
    assert(len(rels) == 4)
    assert(len([rel for rel in rels if rel["p"][0]["id"] == 1 and rel["p"][2]["id"] == 2 and rel["p"][1] == "to"]) == 3)
    assert(len([rel for rel in rels if rel["p"][0]["id"] == 1 and rel["p"][2]["id"] == 2 and rel["p"][1] == "relates to"]) == 1)

def test_merge_nodes_no_pk(session):
    n1 = Node("test", id=1)
    n2 = Node("test", id=2)
    n3 = Node("test", id=2)
    
    graph = n1 | n2 | n3
    with pytest.raises(ValueError): # without primary keys
        merge(graph, session)

def test_merge_nodes_pk_arg(session):
    n1 = Node("test", id=1)
    n2 = Node("test", id=2)
    n3 = Node("test", id=2)
    
    graph = n1 | n2 | n3

    merge(graph, session, primary_label="test", primary_key="id")
    nodes = get_nodes(session)
    assert(len(nodes)==2)
    assert(len([node for node in nodes if node["c"]["id"] == 2]) == 1)
    assert(len([node for node in nodes if node["c"]["id"] == 1]) == 1)

def test_merge_nodes_pk_in_node(session):
    """Additional test for merge_nodes"""
    n1 = Node("test", id=1)
    n2 = Node("test", id=2)
    n3 = Node("test", id=2)
    n4 = Node("test", id=1)
    for n in (n1,n2,n3,n4):
        n.set_primary_key("id")
        n.set_primary_label("test")
    
    graph = n1 | n2 | n3 | n4
    merge(graph, session)
    nodes = get_nodes(session)

    assert(len(nodes)==2)
    assert(len([node for node in nodes if node["c"]["id"] == 2]) == 1)
    assert(len([node for node in nodes if node["c"]["id"] == 1]) == 1)

def test_merge_relationships_no_pk(session):
    n1 = Node("test", id=1)
    n2 = Node("test", id=2)
    r1 = Relationship(n1, "to", n2, id=1)
    
    create(r1, session)
    r2 = Relationship(n1, "to", n2, id=1)
    with pytest.raises(ValueError): # without primary keys
        merge(r2, session)

def test_merge_relationships_pk_arg(session):
    n1 = Node("test", id=1)
    n2 = Node("test", id=2)
    r1 = Relationship(n1, "to", n2, id=1)
    
    create(r1, session)
    r2 = Relationship(n1, "to", n2, id=1)
    merge(r2, session, primary_key="id")
    rels = get_relationships(session=session)
    assert(len(rels) == 1)
    assert(rels[0]["p"][0]["id"] == 1)

def test_merge_relationships_pk_in_rel(session):
    """Additional test for merge_relations"""
    n1 = Node("test", id=1)
    n2 = Node("test", id=2)
    r1 = Relationship(n1, "to", n2, id=1)
    r2 = Relationship(n1, "to", n2, id=2)
    graph = n1 | n2 | r1 | r2
    create(graph, session)
    r3 = Relationship(n1, "to", n2, id=1)
    r3.set_primary_key("id")
    r4 = Relationship(n1, "to", n2, id=2)
    r4.set_primary_key("id")
    graph = r3 | r4
    merge(graph, session)
    rels = get_relationships(session=session)
    assert(len(rels) == 2)
    assert(len([rel for rel in rels if rel["p"][0]["id"] == 1 and rel["p"][2]["id"] == 2 and rel["p"][1] == "to" and rel["props"]["id"] == 1]) == 1)
    assert(len([rel for rel in rels if rel["p"][0]["id"] == 1 and rel["p"][2]["id"] == 2 and rel["p"][1] == "to" and rel["props"]["id"] == 2]) == 1)

def test_MERGE_RELATIONSHIPS(session):
    n1 = Node("test", id=1)
    n2 = Node("test", id=2)
    r1 = Relationship(n1, "to", n2)
    create(Subgraph(nodes=[n1, n2], relationships=[r1]), session)

    r2 = MERGE_RELATIONSHIPS(Relationship(n1, "to", n2))
    merge(Subgraph(relationships=[r2]), session)
    rels = get_relationships(session=session)
    assert(len(rels) == 1)
    assert(len([r for r in rels if r["p"][0]["id"] == 1 and r["p"][2]["id"] == 2 and r["p"][1] == "to"]) == 1)

def test_create_node_with_date(session):
    n1 = Node("test", id=1, date=datetime.date(2020,1,1))
    create(n1, session)
    nodes = get_nodes(session)
    assert(len(nodes) == 1)
    assert(nodes[0]["c"]["id"] == 1)
    assert(nodes[0]["c"]["date"] == time.Date(2020,1,1))

def test_push_node(session):
    n1 = Node("test", id=1)
    create(n1, session)
    n1["id"] = 2
    push(n1, session)
    nodes = get_nodes(session)
    assert(len(nodes) == 1)
    assert(nodes[0]["c"]["id"] == 2)

    # labels
    n1.labels = set(["test", "another"])
    push(n1, session)
    nodes = get_nodes(session)
    assert(len(nodes) == 1)
    assert(nodes[0]["labels(c)"] == ["test", "another"])

    # remove label
    n1.labels = set(["test"])
    push(n1, session)
    nodes = get_nodes(session)
    assert(len(nodes) == 1)
    assert(nodes[0]["labels(c)"] == ["test"])

def test_pull_node(session):
    n1 = Node("test", id=1)
    create(n1, session)
    n1["id"] = 2
    pull(n1, session)
    assert(n1["id"] == 1)

    # remove label
    n1.labels = set(["test", "another"])
    pull(n1, session)
    assert(n1.labels == {"test"})

    # add label
    n1.labels = set(["test", "another"])
    push(n1, session)
    n1.labels = set(["test"])
    pull(n1, session)
    assert(n1.labels == {"test", "another"})


def test_push_relationship(session):
    n1 = Node("test", id=1)
    n2 = Node("test", id=2)
    r1 = Relationship(n1, "to", n2)
    create(r1, session)
    r1["id"] = 2
    push(r1, session)
    rels = get_relationships(session)
    assert(len(rels) == 1)
    assert(rels[0]["props"]["id"] == 2)

def test_pull_relationship(session):
    n1 = Node("test", id=1)
    n2 = Node("test", id=2)
    r1 = Relationship(n1, "to", n2, id=1)
    create(r1, session)
    r1["id"] = 2
    pull(r1, session)
    assert(r1["id"] == 1)