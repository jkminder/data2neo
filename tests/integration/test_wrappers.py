#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Integration tests for wrappers.

authors: Julian Minder
"""

import pytest 
import pandas as pd

from rel2graph import Converter
from rel2graph import AttributeFactoryWrapper, SubgraphFactoryWrapper
from rel2graph.neo4j import Node, Relationship, Subgraph
from rel2graph.utils import load_file
from rel2graph.relational_modules.pandas import PandasDataFrameIterator
from rel2graph import register_wrapper, register_attribute_postprocessor, register_attribute_preprocessor, register_subgraph_postprocessor, register_subgraph_preprocessor
from rel2graph import Attribute

from helpers import *
# Turn off reinstantiation warnings
Converter.no_instantiation_warnings = True


@pytest.fixture
def input():
    return pd.DataFrame({"First": ["F"], "Second": ["S"], "Third": ["T"]})


# Schema file
schema_file = "tests/integration/resources/schema_wrappers.yaml"

##### Register wrappers #####
@register_attribute_preprocessor
def attr_pre_change(resource):
    # change the resource
    resource["First"] = "Changed"
    return resource

@register_attribute_preprocessor
def attr_pre_condition(resource):
    return None # Remove attribute

@register_attribute_preprocessor
def attr_pre_new(resource):
    resource["Forth"] = "F" # add new attr
    return resource

@register_attribute_postprocessor
def attr_post_append(attribute):
    # Append something to the attribute
    new_attr = Attribute(attribute.key, attribute.value + " appendix")
    return new_attr

@register_attribute_postprocessor
def attr_post_append_parametrized(attribute, new_value):
    # Append something to the attribute
    new_attr = Attribute(attribute.key, attribute.value + new_value)
    return new_attr

@register_wrapper
class AttrWrapper(AttributeFactoryWrapper):
    def __init__(self, factory, attribute, another_attribute):
        super().__init__(factory)
        self._attribute = attribute
        self._another_attribute = another_attribute
    
    def construct(self, resource):
        # modify resource
        resource["First"] = self._attribute
        attribute = super().construct(resource)
        # modify attribute
        new_attr = Attribute(self._another_attribute, attribute.key + ":" + attribute.value)
        return new_attr
    
@register_subgraph_preprocessor
def sg_pre_change(resource):
    resource["First"] = "Changed"
    return resource

@register_subgraph_preprocessor
def sg_pre_change_parametrized(resource, key, new_value):
    resource[key] = new_value
    return resource

@register_subgraph_preprocessor
def sg_pre_condition(resource):
    return None

@register_subgraph_postprocessor
def sg_post_add(subgraph):
    first_node = subgraph.nodes[0]
    new_node = Node("From Copy", First=first_node["First"])
    new_rel = Relationship(first_node, "is copied by", new_node)
    return subgraph|new_node|new_rel

@register_subgraph_postprocessor
def sg_post_condition(subgraph):
    return Subgraph() # empty subgraph

@register_wrapper
class SGWrapper(SubgraphFactoryWrapper):
    def __init__(self, factory, attribute, another_attribute):
        super().__init__(factory)
        self._attribute = attribute
        self._another_attribute = another_attribute
    
    def construct(self, resource):
        # modify resource
        resource["Fifth"] = self._attribute
        subgraph = super().construct(resource)
        # modify subgraph
        subgraph.nodes[0]["Sixth"] = self._another_attribute
        return subgraph
############################

@pytest.mark.parametrize(
    "workers",
    [1,5]
)
@pytest.mark.parametrize("batch_size",[1,100])
def test_attr_pre(input, workers, batch_size, session, uri, auth):
    iterator = PandasDataFrameIterator(input, "ATTRPRE")
    converter = Converter(load_file(schema_file), iterator, uri, auth, num_workers=workers, batch_size=batch_size)
    # run 
    converter()
    #compare
    assert num_nodes(session) == 1
    node = get_nodes(session)[0]
    assert node["First"] == "Changed" #attr_pre_change
    assert len(node) == 2 # attr_pre_condition -> one attr removed
    assert node["Third"] == "F" # attr_pre_new

@pytest.mark.parametrize(
    "workers",
    [1,5]
)
@pytest.mark.parametrize("batch_size",[1,100])
def test_attr_post(input, workers, batch_size, session, uri, auth):
    iterator = PandasDataFrameIterator(input, "ATTRPOST")
    converter = Converter(load_file(schema_file), iterator, uri, auth, num_workers=workers, batch_size=batch_size)
    # run 
    converter()
    #compare
    assert num_nodes(session) == 1
    node = get_nodes(session)[0]
    assert "MyType appendix" in node.labels
    assert node["First"] == "F appendix"
    assert node["Second"] == "S appendix appendix" # Chaining
    assert node["Third"] == "Changed appendix" # pre and post
    assert node["Forth"] == "T-value" # parametrized

@pytest.mark.parametrize(
    "workers",
    [1,5]
)
@pytest.mark.parametrize("batch_size",[1,100])
def test_attr_wrapper(input, workers, batch_size, session, uri, auth):
    iterator = PandasDataFrameIterator(input, "ATTRWRAPPER")
    converter = Converter(load_file(schema_file), iterator, uri, auth, num_workers=workers, batch_size=batch_size)
    # run 
    converter()
    #compare
    assert len(get_nodes(session)) == 1
    node = get_nodes(session)[0]
    assert len(node) == 1 # only one 
    assert node["Test2"] == "First:Test1"

@pytest.mark.parametrize(
    "workers",
    [1,5]
)
@pytest.mark.parametrize("batch_size",[1,100])
def test_subgraph_pre(input, workers, batch_size, session, uri, auth):
    iterator = PandasDataFrameIterator(input, "SGPRE")
    converter = Converter(load_file(schema_file), iterator, uri, auth, num_workers=workers, batch_size=batch_size)
    # run 
    converter()
    #compare
    assert len(get_nodes(session)) == 2 # make sure only two node are created
    node_from = [node for node in get_nodes(session) if "From" in node.labels][0]
    print(get_nodes(session))
    assert node_from["First"] == "Changed"
    assert len(get_relationships(session)) == 1
    rel = get_relationships(session)[0]
    assert rel["First"] == "Changed" # The resource was changed earlier -> this is still the case
    assert rel["Second"] == "CHANGED"

@pytest.mark.parametrize(
    "workers",
    [1,5]
)
@pytest.mark.parametrize("batch_size",[1,100])
def test_subgraph_post(input, workers, batch_size, session, uri, auth):
    iterator = PandasDataFrameIterator(input, "SGPOST")
    converter = Converter(load_file(schema_file), iterator, uri, auth, num_workers=workers, batch_size=batch_size)
    # run 
    converter()
    #compare
    assert len(get_nodes(session)) == 2 # make sure only two node are created
    node_from = [node for node in get_nodes(session) if "From" in node.labels][0] # must exist
    node_copy = [node for node in get_nodes(session) if "From Copy" in node.labels][0] # must exist
    assert node_from["First"] == "F"
    assert node_copy["First"] == "F"
    assert len(get_relationships(session)) == 1
    rel = get_relationships(session)[0]
    assert rel.type == "is copied by"
    assert "From" in rel.start_node.labels
    assert "From Copy" in rel.end_node.labels

@pytest.mark.parametrize(
    "workers",
    [1,5]
)
@pytest.mark.parametrize("batch_size",[1,100])
def test_subgraph_wrapper(input, workers, batch_size, session, uri, auth):
    iterator = PandasDataFrameIterator(input, "SGWRAPPER")
    converter = Converter(load_file(schema_file), iterator, uri, auth, num_workers=workers, batch_size=batch_size)
    # run 
    converter()
    #compare
    assert len(get_nodes(session)) == 1 # make sure only one node is created
    node = get_nodes(session)[0] # must exist
    assert node["First"] == "F"
    assert node["Fifth"] == "Test1"
    assert node["Sixth"] == "Test2"