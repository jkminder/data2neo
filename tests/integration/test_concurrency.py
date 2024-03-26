#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Integration tests for testing the synchronous conversion of relations. Related to issue #20.

authors: Julian Minder
"""

import pytest 

import pandas as pd
import logging
import numpy as np

from rel2graph import Converter, IteratorIterator, register_attribute_postprocessor, Attribute, register_subgraph_preprocessor, GlobalSharedState, register_subgraph_postprocessor
from rel2graph.utils import load_file
from rel2graph.relational_modules.pandas import PandasDataFrameIterator
from rel2graph.neo4j import match_relationships, push, pull
from rel2graph.common_modules import MERGE_RELATIONSHIPS

from helpers import *


# As this issue is related to multiple workers, we repeat the test multiple times
@pytest.mark.parametrize('execution_number', range(10))
def test_concurrent_relationships(execution_number, session, uri, auth):
    schema = load_file("tests/integration/resources/schema_concurrency.yaml")

    entities = pd.DataFrame({"uid": range(40)})

    # 120 relations between 20 entities
    relations = pd.DataFrame({"from": list(range(20))*6, "to": [i+20 for i in range(20) for _ in range(6)]})
    unique_pairs = len(relations.drop_duplicates())
    print(unique_pairs)

    iterator = IteratorIterator([PandasDataFrameIterator(entities, "Entity"), PandasDataFrameIterator(relations, "Relationship")])

    converter = Converter(schema, iterator, uri, auth, num_workers=12, batch_size=10)
    converter(skip_relationships=False)


    assert num_relationships(session) == 120+unique_pairs
    assert num_nodes(session) == 40