#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Attribute postprocessor for type conversions
authors: Julian Minder
"""

from .. import register_attribute_postprocessor
from .. import Attribute

@register_attribute_postprocessor
def INT(attribute: Attribute) -> Attribute:
    return Attribute(attribute.key, int(attribute.value))

@register_attribute_postprocessor
def FLOAT(attribute: Attribute) -> Attribute:
    return Attribute(attribute.key, float(attribute.value))

@register_attribute_postprocessor
def STR(attribute: Attribute) -> Attribute:
    return Attribute(attribute.key, str(attribute.value))

@register_attribute_postprocessor
def BOOL(attribute: Attribute) -> Attribute:
    return Attribute(attribute.key, bool(attribute.value))
