#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Subgraph preprocessors that only construct if an condition is given

authors: Julian Minder
"""
import warnings
from .. import register_subgraph_preprocessor

@register_subgraph_preprocessor
def CONDITION_EQ(resource, attribute, value):
    if (str(resource[attribute]) != str(value)):
        return None
    return resource

@register_subgraph_preprocessor
def CONDITION_GREATER(resource, attribute, value):
    if (resource[attribute] > value):
        return resource
    return None

@register_subgraph_preprocessor
def CONDITION_SMALLER(resource, attribute, value):
    if (resource[attribute] < value):
        return resource
    return None

#TODO: This allows to inject code -> be aware!
@register_subgraph_preprocessor
def CONDITION(resource, statement):
    # unescape for eval
    statement = statement.replace("\\", "")
    if eval(statement):
        return resource
    return None