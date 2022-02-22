#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Attribute postprocessor that construct dates/datetimes from strings
authors: Julian Minder
"""
from .. import register_attribute_postprocessor
from .. import Attribute
from datetime import datetime

@register_attribute_postprocessor
def DATETIME(attribute, format_string="%Y-%m-%dT%H:%M:%S"):
    if isinstance(attribute.value, datetime):
        return Attribute(attribute.key, attribute.value.replace(tzinfo=None))
    return Attribute(attribute.key, datetime.strptime(attribute.value, format_string))

@register_attribute_postprocessor
def DATE(attribute, format_string="%Y-%m-%dT%H:%M:%S"):
    if isinstance(attribute.value, datetime):
        return Attribute(attribute.key, attribute.value.replace(tzinfo=None).date())
    return Attribute(attribute.key, datetime.strptime(attribute.value, format_string).date())