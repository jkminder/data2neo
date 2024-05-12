#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Integration tests for testing the py2neo extensions and behavior of parallel relationships.
This is especially relevant due to the batch processing.

authors: Julian Minder
"""

import pytest 

import pandas as pd
import neo4j
import os

from data2neo import Converter, IteratorIterator, register_attribute_postprocessor, Attribute
from data2neo.relational_modules.pandas import PandasDataFrameIterator

from helpers import *

@register_attribute_postprocessor
def INT(attribute):
    return Attribute(attribute.key, int(attribute.value))


        
@pytest.mark.parametrize("workers",[1,5])
@pytest.mark.parametrize("batch_size",[1,100])
def test_dependency_between_two_nodes_one_resource_type(workers, batch_size, session, uri, auth):
    schema = """
    ENTITY("Entity"):
        NODE("Entity", "FirstLabel"):
            + id = INT(Entity.id)

        NODE("Entity", "AnotherLabel"):
            + id = INT(Entity.id)
    """
    entities = pd.DataFrame({"id": [1]*1000}) # the start buffer size is 100 so this will require two several batches
    iterator = PandasDataFrameIterator(entities, "Entity")
    converter = Converter(schema, iterator, uri, auth, num_workers=workers, batch_size=batch_size)
    converter()
    assert num_nodes(session) == 1
    assert get_nodes(session, "Entity")[0].labels == {"Entity", "FirstLabel", "AnotherLabel"}


@pytest.mark.parametrize("workers",[1,5])
@pytest.mark.parametrize("batch_size",[1,100])
def test_dependency_between_two_nodes_two_resource_types(workers, batch_size, session, uri, auth):
    schema = """
    ENTITY("Entity"):
        NODE("Entity", "FirstLabel"):
            + id = INT(Entity.id)

    ENTITY("Other"):
        NODE("Entity", "AnotherLabel"):
            + id = INT(Entity.id)
    """
    entities = pd.DataFrame({"id": [1]*1000}) # the start buffer size is 100 so this will require two several batches
    iterator = IteratorIterator([PandasDataFrameIterator(entities, "Entity"), PandasDataFrameIterator(entities, "Other")])
    converter = Converter(schema, iterator, uri, auth, num_workers=workers, batch_size=batch_size)
    converter()
    assert num_nodes(session) == 1
    assert get_nodes(session, "Entity")[0].labels == {"Entity", "FirstLabel", "AnotherLabel"}

