#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Attribute preprocessors that raises assert exceptions if preconditions aren't given

authors: Julian Minder
"""

from os import stat
from .. import register_attribute_postprocessor
from .. import Attribute

@register_attribute_postprocessor
def ASSERT_EQ(attribute: Attribute, value: str) -> Attribute:
    assert attribute.value == value, f"Attribute {attribute.key} '{attribute.value}' != '{value}'"
    return attribute

@register_attribute_postprocessor
def ASSERT_BIGGER(attribute: Attribute, value: str) -> Attribute:
    assert attribute.value > value, f"Attribute {attribute.key} '{attribute.value}' <= '{value}'"
    return attribute

@register_attribute_postprocessor
def ASSERT_SMALLER(attribute: Attribute, value: str) -> Attribute:
    assert attribute.value < value, f"Attribute {attribute.key} '{attribute.value}' >= '{value}'"
    return attribute

@register_attribute_postprocessor
def ASSERT_CONTAINS(attribute: Attribute, value: str) -> Attribute:
    assert value in attribute.value, f"Attribute {attribute.key} '{attribute.value}' does not contain '{value}'"
    return attribute

@register_attribute_postprocessor
def ASSERT_STARTSWITH(attribute: Attribute, value: str) -> Attribute:
    assert attribute.value.startswith(value) , f"Attribute {attribute.key} '{attribute.value}' does not start with '{value}'"
    return attribute

@register_attribute_postprocessor
def ASSERT_ENDSWITH(attribute: Attribute, value: str) -> Attribute:
    assert attribute.value.endswith(value), f"Attribute {attribute.key} '{attribute.value}' does not end with '{value}'"
    return attribute

#TODO: This allows to inject code -> be aware!
@register_attribute_postprocessor
def ASSERT(attribute: Attribute, statement: str) -> Attribute:
    """Asserts a statement. The statement is executed in the scope of this 
    function and can therefore access the attribute."""
    # unescape for eval
    statement = statement.replace("\\", "")
    assert eval(statement), f"Attribute {attribute.key}: '{attribute.value}' - '{statement}' returned False"
    return attribute

