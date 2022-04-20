#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tests for OData relational module

TODO: tests heavily access protected members of the library -> improve? how?

authors: Julian Minder
"""

import pytest

from rel2graph.core.schema_compiler import compile_schema, SchemaConfigParser, _precompile
from rel2graph import register_attribute_preprocessor, SchemaConfigException

######## TESTS PRECOMPILER ##########

def test_precompile_commentremoval():
    """Tests if precompilation correctly removes all comments from the schema config string."""
    input_string = """
    ENTITY("entity"):
    WRAPPER(NODE("label", WRAP("label2"), WRAP("label3", 1234), entity.column), "someargument", 123) nodeid: # some comment
        + test = entity.column #######
        - test1 = "static \\" string" ##ka asdflkjasdölfj
        - test2 = WRAP2(WRAP(entity.col))
    RELATION(MATCH("label", "label2", name="test", id=WRAP(test.idcolumn)), "type", to):
        + test = entity.column # testi 123 vier fünf
        - test1 = "static \\" string" 
        - test2 = WRAP2(WRAP(entity.col))
    # this is another comment ,!'_
    ENTITY("second"):
        RELATION(MATCH("label", "label2", name="test", id=WRAP(test.idcolumn)), "type", to):
        + test = entity.column # this is a coomment
        - test1 = "static \\" string"
        - test2 = WRAP2(WRAP(entity.col))
    ENTITY("third"):
    """
    precompiled_string = """
    ENTITY("entity"):
    WRAPPER(NODE("label", WRAP("label2"), WRAP("label3", 1234), entity.column), "someargument", 123) nodeid: 
        + test = entity.column 
        - test1 = "static \\" string" 
        - test2 = WRAP2(WRAP(entity.col))
    RELATION(MATCH("label", "label2", name="test", id=WRAP(test.idcolumn)), "type", to):
        + test = entity.column 
        - test1 = "static \\" string" 
        - test2 = WRAP2(WRAP(entity.col))
    
    ENTITY("second"):
        RELATION(MATCH("label", "label2", name="test", id=WRAP(test.idcolumn)), "type", to):
        + test = entity.column 
        - test1 = "static \\" string"
        - test2 = WRAP2(WRAP(entity.col))
    ENTITY("third"):
    """
    assert _precompile(input_string) == precompiled_string

######## TESTS PARSER ##########

def test_parser_complex():
    """Tests correct parsing of a complex schema definition."""
    input_string = """
    ENTITY("entity"):
    WRAPPER(NODE("label", WRAP("label2"), WRAP("label3", 1234), entity.column), "someargument", 123) nodeid:
        + test = entity.column
        - test1 = "static \\" string"
        - test2 = WRAP2(WRAP(entity.col))
    RELATION(MATCH("label", "label2", name="test", id=WRAP(test.idcolumn)), "type", to):
        + test = entity.column
        - test1 = "static \\" string"
        - test2 = WRAP2(WRAP(entity.col))
    ENTITY("second"):
        RELATION(MATCH("label", "label2", name="test", id=WRAP(test.idcolumn)), "type", to):
        + test = entity.column
        - test1 = "static \\" string"
        - test2 = WRAP2(WRAP(entity.col))
    ENTITY("third"):
    """
    ground_truth = [['entity', [[['WRAPPER', [['NodeFactory', [[['AttributeFactory', ['test', 'column', None]], ['AttributeFactory', ['test1', None, 'static \\" string']], ['WRAP2', [['WRAP', [['AttributeFactory', ['test2', 'col', None]]]]]]], [['AttributeFactory', [None, None, 'label']], ['WRAP', [['AttributeFactory', [None, None, 'label2']]]], ['WRAP', [['AttributeFactory', [None, None, 'label3']], ['AttributeFactory', [None, None, 1234]]]], ['AttributeFactory', [None, 'column', None]]], 'test', 'nodeid']], ['AttributeFactory', [None, None, 'someargument']], ['AttributeFactory', [None, None, 123]]]]], [['RelationFactory', [[['AttributeFactory', ['test', 'column', None]], ['AttributeFactory', ['test1', None, 'static \\" string']], ['WRAP2', [['WRAP', [['AttributeFactory', ['test2', 'col', None]]]]]]], ['AttributeFactory', [None, None, 'type']], ['Matcher', [None, ['AttributeFactory', [None, None, 'label']], ['AttributeFactory', [None, None, 'label2']], ['AttributeFactory', ['name', None, 'test']], ['WRAP', [['AttributeFactory', ['id', 'idcolumn', None]]]]]], ['Matcher', ['to']], 'test', None]]]]], ['second', [[], [['RelationFactory', [[['AttributeFactory', ['test', 'column', None]], ['AttributeFactory', ['test1', None, 'static \\" string']], ['WRAP2', [['WRAP', [['AttributeFactory', ['test2', 'col', None]]]]]]], ['AttributeFactory', [None, None, 'type']], ['Matcher', [None, ['AttributeFactory', [None, None, 'label']], ['AttributeFactory', [None, None, 'label2']], ['AttributeFactory', ['name', None, 'test']], ['WRAP', [['AttributeFactory', ['id', 'idcolumn', None]]]]]], ['Matcher', ['to']], 'test', None]]]]], ['third', [[], []]]]
    parser = SchemaConfigParser()
    assert ground_truth == parser.parse(input_string)

def test_parser_nodes_with_same_labels():
    """Tests if nodes are specified with the exact same config string are merged. This test verifies the libraries fix for github issue #2."""
    input_string = """
    ENTITY("LegislativePeriod"):
    NODE("Source"):
        + name = "Online DB"
    NODE("Source"):
        + name = "Amtliche Sammlung"
    NODE("Source"):
        + name = "Bundesblatt"
    """
    ground_truth = [['LegislativePeriod', [[['NodeFactory', [[['AttributeFactory', ['name', None, 'Online DB']]], [['AttributeFactory', [None, None, 'Source']]], 'name', None]], ['NodeFactory', [[['AttributeFactory', ['name', None, 'Amtliche Sammlung']]], [['AttributeFactory', [None, None, 'Source']]], 'name', None]], ['NodeFactory', [[['AttributeFactory', ['name', None, 'Bundesblatt']]], [['AttributeFactory', [None, None, 'Source']]], 'name', None]]], []]]]
    parser = SchemaConfigParser()
    assert ground_truth == parser.parse(input_string)

def test_parser_overlapping_identifiers():
    """Tests if overlapping identifiers are correctly parsed (e.g. year vs year_end). This test verifies the libraries fix for github issue #1."""
    input_string = """
    ENTITY("Session"):
    NODE("Year") year:
    NODE("Year") year_end:
    """
    ground_truth = [['Session', [[['NodeFactory', [[], [['AttributeFactory', [None, None, 'Year']]], None, 'year']], ['NodeFactory', [[], [['AttributeFactory', [None, None, 'Year']]], None, 'year_end']]], []]]]
    parser = SchemaConfigParser()
    assert ground_truth == parser.parse(input_string)

def test_parser_raises_identifier_twice():
    """Tests if parser correctly raises an exception if an identifier is defined twice."""
    input_string = """
    ENTITY('entity'):
        NODE("label") node:
        NODE("label2") node:
    """
    with pytest.raises(SchemaConfigException) as excinfo:
        parser = SchemaConfigParser()
        parser.parse(input_string)
    exception_msg = excinfo.value.args[0]
    assert exception_msg == "Found conflicting definitions of identifiers ['node'] in entity 'entity'. An identifier must be unique."

def test_parser_raises_two_primary_keys():
    """Test if parser correctly raises an exception if a graphelement has two defined primary keys."""
    input_string = """
    ENTITY('entity'):
        NODE("label") node:
            + name = entity.attr
            + name2 = entity.attr
    """
    with pytest.raises(SchemaConfigException) as excinfo:
        parser = SchemaConfigParser()
        parser.parse(input_string)
    exception_msg = excinfo.value.args[0]
    assert exception_msg == "Setting two or more primary keys for one graphelement is not allowed. Conflict: 'name' <-> 'name2'"

def test_parser_raises_illegal_character():
    """Test if parser correctly raises an exception if a the schema contains an illegal character."""
    input_string = """
    ENTITY('entity') "whatisthis:
        NODE("label") node:
    """
    with pytest.raises(SchemaConfigException) as excinfo:
        parser = SchemaConfigParser()
        parser.parse(input_string)
    exception_msg = excinfo.value.args[0]
    assert exception_msg.startswith("Illegal character '\"' on line 2")

def test_parser_raises_illegal_token():
    """Test if parser correctly raises an exception if a the schema contains an invalid token."""
    input_string = """
    ENTITY('entity'):
        ENTITY("label") node:
    """
    with pytest.raises(SchemaConfigException) as excinfo:
        parser = SchemaConfigParser()
        parser.parse(input_string)
    exception_msg = excinfo.value.args[0]
    assert exception_msg.startswith("Couldn't resolve token 'node' at position")
######## TESTS FULL COMPILER ##########

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

def test_full_compiler_matcher_conditions():
    """Tests if conditions of matcher (dynamic and static) are parsed and compiled correctly"""
    relation_supplychain = compile_schema(get_filepath("matcher_condition"))["entity"][1] # get relation supplychain
    for rf in relation_supplychain.factories:
        type =  get_rel_type(rf) 
        assert type in ["static-dyn", "static", "two-static", "dyn", "two-dyn", "two-dyn-two-static"]
        # check from matcher
        fm = rf._from_matcher
        assert fm._node_id == "identifier"
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
        
def test_full_compiler_node_primary():
    """Test if primary keys for nodes are correct parsed"""
    node_supplychain, _ = compile_schema(get_filepath("primary_keys"))["entity"]
     
    for nf in node_supplychain.factories:
        labels = get_labels(nf)
        assert(len(labels) == 1)
        label = labels[0]
        assert(label in ["noattr", "nopk", "pk"])
        if label in ["noattr", "nopk"]:
            assert(nf._primary_key is None)
        if label in ["pk"]:
            assert(nf._primary_key == "pk")

def test_full_compiler_relations_primary():
    """Test if primary keys for relations are correct parsed"""
    _, relation_supplychain = compile_schema(get_filepath("primary_keys"))["entity"]
     
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

def test_full_compiler_typing():
    """Test if different types for static arguments are correctly parsed"""
    node_supplychain, relation_supplychain = compile_schema(get_filepath("typing"))["entity"]

    for nf in node_supplychain.factories:
        attributes = af2str(nf._attributes)
        check_types(attributes)

    for rf in relation_supplychain.factories:
        # check to matcher
        fm = rf._to_matcher
        conditions = af2str(fm._conditions)
        check_types(conditions)

def test_full_compiler_dynkeys():
    node_supplychain, relation_supplychain = compile_schema(get_filepath("dynamic_keys"))["entity"]
    
    for nf in node_supplychain.factories:
        for label in nf._labels:
            assert(label._entity_attribute == "dynamic_key")
        for attr in nf._attributes:
            assert(attr._entity_attribute == "dynamic_key")
    
    for rf in relation_supplychain.factories:
        assert(rf._type._entity_attribute == "dynamic_key")
        for attr in nf._attributes:
            assert(attr._entity_attribute == "dynamic_key")
        
def test_full_compiler_empty_entity():
    node_supplychain, relation_supplychain = compile_schema(get_filepath("empty_entity"))["entity"]
    assert len(node_supplychain.factories) == 0 
    assert len(relation_supplychain.factories) == 0

def test_compiler_raises_same_entity_twice():
    """Make sure compiler raises exception when defining an entity twice."""
    with pytest.raises(SchemaConfigException) as excinfo:
        compile_schema(get_filepath("conflicting_entities"))
    exception_msg = excinfo.value.args[0]
    assert exception_msg == "Found two conflicting definitions of entity 'entity'. Please only specify each entity once."