#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tests for OData relational module

TODO: tests heavily access protected members of the library -> improve? how?

authors: Julian Minder
"""

import pytest

from rel2graph.core.config_parser import parse
from rel2graph import register_attribute_preprocessor

@register_attribute_preprocessor
def WRAPPER(resource):
    return resource

def get_rel_type(relation_factory):
    return relation_factory._type.static_attribute_value

def get_labels(node_factory):
    return [af.static_attribute_value for af in node_factory._labels]

def af2str(list_of_af):
    """attribute factories to strings"""
    return [(af.attribute_key, (af.entity_attribute if af.entity_attribute is not None else af.static_attribute_value)) for af in list_of_af]

def get_filepath(name):
    return f"tests/unit/core/resources/{name}.yaml"

def test_matcher_conditions():
    """Tests if conditions of matcher (dynamic and static) are parsed and compiled correctly"""
    relation_supplychain = parse(get_filepath("matcher_condition"))["entity"][1] # get relation supplychain
    for rf in relation_supplychain.factories:
        type =  get_rel_type(rf) 
        assert type in ["static-dyn", "static", "two-static", "dyn", "two-dyn", "two-dyn-two-static"]
        # check from matcher
        fm = rf._from_matcher
        assert fm._node_id == "entity.identifier"
        # check to matcher with static conditions
        tm = rf._to_matcher
        assert (None, "entity") in af2str(tm._labels) # check labels
        if "static" in type:
            assert ("Name", "static") in af2str(tm._conditions) # check first static condition
        if "two-static" in type:
            assert ("Name2", "static2") in af2str(tm._conditions) # check second static condition
        if "dyn" in type:
            assert ("Dyn", "dyn") in af2str(tm._conditions) # check first dyn condition
        if "two-dyn" in type:
            assert ("Dyn2", "dyn2") in af2str(tm._conditions) # check second dyn condition
        
def test_node_primary():
    """Test if primary keys for nodes are correct parsed"""
    node_supplychain, _ = parse(get_filepath("primary_keys"))["entity"]
     
    for nf in node_supplychain.factories:
        labels = get_labels(nf)
        assert(len(labels) == 1)
        label = labels[0]
        assert(label in ["noattr", "nopk", "pk"])
        if label in ["noattr", "nopk"]:
            assert(nf._primary_key is None)
        if label in ["pk"]:
            assert(nf._primary_key == "pk")

def test_relations_primary():
    """Test if primary keys for relations are correct parsed"""
    _, relation_supplychain = parse(get_filepath("primary_keys"))["entity"]
     
    for rf in relation_supplychain.factories:
        type =  get_rel_type(rf) 

        assert(type in ["noattr", "nopk", "pk"])
        if type in ["noattr", "nopk"]:
            assert(rf._primary_key is None)
        if type in ["pk"]:
            assert(rf._primary_key == "pk")

def check_types(list_of_attribuets):
    for (key, value) in list_of_attribuets:
            if key == "myint":
                assert(isinstance(value, int) and value==1)
            elif key == "mystr":
                assert(isinstance(value, str) and value=="1")
            elif key == "myfloat":
                assert(isinstance(value, float) and value==1.1)
            elif key == "myTrue":
                assert(isinstance(value, bool) and value)
            elif key == "myFalse":
                assert(isinstance(value, bool) and not value)
            else:
                assert(False) # not possible

def test_typing():
    """Test if different types for static arguments are correctly parsed"""
    node_supplychain, relation_supplychain = parse(get_filepath("typing"))["entity"]

    for nf in node_supplychain.factories:
        attributes = af2str(nf._attributes)
        check_types(attributes)

    for rf in relation_supplychain.factories:
        # check to matcher
        fm = rf._to_matcher
        conditions = af2str(fm._conditions)
        check_types(conditions)

def test_dynkeys():
    node_supplychain, relation_supplychain = parse(get_filepath("dynamic_keys"))["entity"]
    
    for nf in node_supplychain.factories:
        for label in nf._labels:
            assert(label._entity_attribute == "dynamic_key")
        for attr in nf._attributes:
            assert(attr._entity_attribute == "dynamic_key")
    
    for rf in relation_supplychain.factories:
        assert(rf._type._entity_attribute == "dynamic_key")
        for attr in nf._attributes:
            assert(attr._entity_attribute == "dynamic_key")