
#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tests for Matcher.

authors: Julian Minder
"""

import pytest
import neo4j
import os

from rel2graph.core.factories import Matcher, AttributeFactory
from rel2graph import Resource

def delete_all(session):
    session.run("MATCH (n) DETACH DELETE n")

def generate_test_nodes(session):
    # Create nodes
    session.run("CREATE (n:TestLabel:A {id: 1, id2: 10})")
    session.run("CREATE (n:TestLabel:B {id: 2})")
    session.run("CREATE (n:OtherLabel {id: 1})")


class DummyResource(Resource):
    def __init__(self, properties):
        super().__init__()
        self.properties = properties

    @property
    def type(self) -> str:
        return "Dummy"
    
    def __repr__(self) -> str:
        return super().__repr__()
    
    def __getitem__(self, key: str) -> str:
        return self.properties[key]
    
    def __setitem__(self, key: str, value: str) -> None:
        self.properties[key] = value

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
            generate_test_nodes(session)
            Matcher.graph_driver = driver
            print(Matcher.graph_driver)
            yield session
        finally:
            session.close()
            return

def test_matcher_labels_attributes(session):
    # Static labels no attributes
    label_factory = AttributeFactory(None, None, "TestLabel")
    dummy_resource = DummyResource({"id": 1})
    matcher = Matcher(None, label_factory)
    match = matcher.match(dummy_resource)
    assert len(match) == 2
    assert set([m["id"] for m in match]) == set([1, 2])

    # Two static labels no attributes
    label_factory = AttributeFactory(None, None, "TestLabel")
    label_factory2 = AttributeFactory(None, None, "A")
    dummy_resource = DummyResource({"id": 1})
    matcher = Matcher(None, label_factory, label_factory2)
    match = matcher.match(dummy_resource)
    assert len(match) == 1
    assert match[0]["id"] == 1
    
    # Static labels with attributes
    label_factory = AttributeFactory(None, None, "TestLabel")
    attr_factory = AttributeFactory("id", "id")
    dummy_resource = DummyResource({"id": 1})
    matcher = Matcher(None, label_factory, attr_factory)
    match = matcher.match(dummy_resource)
    assert len(match) == 1
    assert match[0]["id"] == 1

    # Static labels with multiple attributes
    label_factory = AttributeFactory(None, None, "TestLabel")
    attr_factory = AttributeFactory("id", "id")
    attr_factory2 = AttributeFactory("id2", "id2")
    dummy_resource = DummyResource({"id": 1, "id2": 10})
    matcher = Matcher(None, label_factory, attr_factory, attr_factory2)
    match = matcher.match(dummy_resource)
    assert len(match) == 1
    assert match[0]["id"] == 1
    assert match[0]["id2"] == 10

    # Static labels with attributes (no match)
    label_factory = AttributeFactory(None, None, "TestLabel")
    attr_factory = AttributeFactory("id", "id")
    dummy_resource = DummyResource({"id": 4})
    matcher = Matcher(None, label_factory, attr_factory)
    match = matcher.match(dummy_resource)
    assert len(match) == 0

    #Dynamic labels no attributes
    label_factory = AttributeFactory(None, "Label", None)
    dummy_resource = DummyResource({"Label": "TestLabel"})
    matcher = Matcher(None, label_factory)
    match = matcher.match(dummy_resource)
    assert len(match) == 2
    assert set([m["id"] for m in match]) == set([1, 2])

    #Dynamic labels with attributes
    label_factory = AttributeFactory(None, "Label", None)
    attr_factory = AttributeFactory("id", "id")
    dummy_resource = DummyResource({"Label": "TestLabel", "id": 1})
    matcher = Matcher(None, label_factory, attr_factory)
    match = matcher.match(dummy_resource)
    assert len(match) == 1
    assert match[0]["id"] == 1

def test_matcher_nodeid(session):
    dummy_resource = DummyResource({"id": 1})
    dummy_resource.supplies["test"] = "THIS IS A TEST"

    matcher = Matcher("test")

    match = matcher.match(dummy_resource)
    assert len(match) == 1
    assert match[0] == "THIS IS A TEST"