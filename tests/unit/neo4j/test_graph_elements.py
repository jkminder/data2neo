#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tests for graph elements.

authors: Julian Minder
"""

import os
import pytest
import pickle

from rel2graph.neo4j import Node, Relationship, Subgraph
from rel2graph import Attribute

def test_attributes():
    attr = Attribute("key", "value")
    assert attr.key == "key"
    assert attr.value == "value"

    # unmodifiable
    with pytest.raises(AttributeError):
        attr.key = "key2"
    with pytest.raises(AttributeError):
        attr.value = "value2"

    
def test_nodes():
    # no attributes
    n1 = Node("test")
    assert "test" in n1.labels

    # single label single attribute
    n1 = Node("test", id=1)
    assert n1["id"] == 1
    assert "test" in n1.labels

    # single label multiple attributes
    n2 = Node("test", id=2, name="test")
    assert n2["id"] == 2
    assert n2["name"] == "test"
    assert "test" in n2.labels

    # multiple labels
    n3 = Node("test", "test2", id=3)
    assert n3["id"] == 3
    assert "test" in n3.labels
    assert "test2" in n3.labels

    # multiple labels multiple attributes
    n4 = Node("test", "test2", id=4, name="test")
    assert n4["id"] == 4
    assert n4["name"] == "test"
    assert "test" in n4.labels
    assert "test2" in n4.labels
    assert "id" in n4.keys()
    assert "name" in n4.keys()
    assert "id" in n4
    assert "name" in n4

    # test primary key
    n5 = Node("test", id=5)
    n5.set_primary_key("id")
    assert n5["id"] == 5
    assert n5.__primarykey__ == "id"
    
    # test primary key exception
    with pytest.raises(ValueError):
        n5.set_primary_key("id2")

    # test primary label
    assert n4.__primarylabel__ == "test"
    n4.set_primary_label("test2")
    assert n4.__primarylabel__ == "test2"
    
    # test primary label exception
    with pytest.raises(ValueError):
        n4.set_primary_label("test3")


    # test equality
    assert n1 == n1
    assert n1 != n2
    
    n1.identity = 1
    n2.identity = 1
    assert n1 == n2

    # test hash
    assert hash(n1) == hash(n2)
    assert hash(n1) != hash(n3)

    # test non existing attribute
    assert n1["non_existing"] is None

def test_relationships():
    n1 = Node("test", id=1)
    n2 = Node("test", id=2)

    # no attributes
    r1 = Relationship(n1, "to", n2)
    assert r1.start_node == n1
    assert r1.end_node == n2
    assert r1.type == "to"
    assert r1.__primarykey__ == None
    assert n1 in r1.nodes
    assert n2 in r1.nodes

    # attributes
    r2 = Relationship(n1, "to", n2, id=1)
    assert r2.start_node == n1
    assert r2.end_node == n2
    assert r2.type == "to"
    assert r2["id"] == 1
    assert r2.__primarykey__ == None

    # primary key
    r2.set_primary_key("id")
    assert r2.__primarykey__ == "id"

    # primary key exception
    with pytest.raises(ValueError):
        r2.set_primary_key("id2")

    # test equality
    assert r1 == r1
    assert r1 != r2
    r2.identity = 1
    r1.identity = 1
    assert r1 == r2

    # test hash
    assert hash(r1) == hash(r2)
    r2.identity = None
    r1.identity = None
    assert hash(r1) != hash(r2)

    # test non existing attribute
    r1["non_existing"] is None

    
def test_nodes_from_attributes():
    # no attributes
    n1 = Node.from_attributes([Attribute(None, "value")])
    assert "value" in n1.labels

    # single label single attribute
    n1 = Node.from_attributes([Attribute(None, "value")], [Attribute("id", 1)])
    assert n1["id"] == 1
    assert "value" in n1.labels

    # single label multiple attributes
    n2 = Node.from_attributes([Attribute(None, "value")], [Attribute("id", 2), Attribute("name", "test")])
    assert n2["id"] == 2
    assert n2["name"] == "test"
    assert "value" in n2.labels

    # multiple labels
    n3 = Node.from_attributes([Attribute(None, "value"), Attribute(None, "anotherlabel")], [Attribute("id", 3)])
    assert n3["id"] == 3
    assert "value" in n3.labels
    assert "anotherlabel" in n3.labels

    # primary key
    n4 = Node.from_attributes([Attribute(None, "value")], [Attribute("id", 4)], primary_key="id")
    assert n4["id"] == 4
    assert n4.__primarykey__ == "id"
    assert "value" in n4.labels

    # primary label
    n5 = Node.from_attributes([Attribute(None, "value")], [Attribute("id", 5)], primary_label="value")
    assert n5["id"] == 5
    assert n5.__primarylabel__ == "value"


def test_relationships_from_attributes():
    n1 = Node("test", id=1)
    n2 = Node("test", id=2)

    # no attributes
    r1 = Relationship.from_attributes(n1, Attribute(None, "to"), n2)
    assert r1.start_node == n1
    assert r1.end_node == n2
    assert r1.type == "to"

    # attributes
    r2 = Relationship.from_attributes(n1, Attribute(None, "to"), n2, [Attribute("id", 1)])
    assert r2.start_node == n1
    assert r2.end_node == n2
    assert r2.type == "to"
    assert r2["id"] == 1

    # multiple attributes
    r3 = Relationship.from_attributes(n1, Attribute(None, "to"), n2, [Attribute("id", 1), Attribute("name", "test")])
    assert r3.start_node == n1
    assert r3.end_node == n2
    assert r3.type == "to"
    assert r3["id"] == 1
    assert r3["name"] == "test"

    # primary key
    r4 = Relationship.from_attributes(n1, Attribute(None, "to"), n2,  [Attribute("id", 1)], primary_key="id")
    assert r4.start_node == n1
    assert r4.end_node == n2
    assert r4.type == "to"
    assert r4["id"] == 1
    assert r4.__primarykey__ == "id"

    
def test_subgraph():
    n1 = Node("test", id=1)
    n2 = Node("test", id=2)
    n3 = Node("test", id=3)
    r1 = Relationship(n1, "to", n2)
    r2 = Relationship(n1, "another", n2)

    # empty subgraph
    sg1 = Subgraph()
    assert len(sg1.nodes) == 0
    assert len(sg1.relationships) == 0

    # single node
    sg2 = Subgraph(nodes=[n1])
    assert len(sg2.nodes) == 1
    assert len(sg2.relationships) == 0

    # multiple nodes
    sg3 = Subgraph(nodes=[n1, n2])
    assert len(sg3.nodes) == 2
    assert len(sg3.relationships) == 0
    assert n1 in sg3.nodes
    assert n2 in sg3.nodes

    # single relationship
    sg4 = Subgraph(relationships=[r1])
    assert len(sg4.nodes) == 2
    assert len(sg4.relationships) == 1

    # multiple relationships
    sg5 = Subgraph(nodes=[n3], relationships=[r1, r2])
    assert len(sg5.nodes) == 3
    assert len(sg5.relationships) == 2

    # test union
    sg6 = sg2 | sg3
    assert len(sg6.nodes) == 2

    # test union with duplicates
    sg7 = sg2 | sg5
    assert len(sg7.nodes) == 3
    assert len(sg7.relationships) == 2

    


def test_serializable():
    # With identity
    n1 = Node("test", id=1)
    n1.identity = 1

    # test pickle
    n1_pickle = pickle.dumps(n1)
    n1_unpickle = pickle.loads(n1_pickle)
    assert n1 == n1_unpickle
    assert n1.identity == n1_unpickle.identity

    # No identity
    n2 = Node("test", id=2)
    
    # test pickle
    n2_pickle = pickle.dumps(n2)
    n2_unpickle = pickle.loads(n2_pickle)
    assert n2.properties == n2_unpickle.properties
    assert n2.labels == n2_unpickle.labels

    # Relationship
    n3 = Node("test", id=3)
    n4 = Node("test", id=4)
    r1 = Relationship(n3, "to", n4)
    r1.identity = 1

    # test pickle
    r1_pickle = pickle.dumps(r1)
    r1_unpickle = pickle.loads(r1_pickle)

    assert r1 == r1_unpickle
    assert r1.identity == r1_unpickle.identity

