#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Unit tests for converter class.

authors: Julian Minder
"""


import pytest
import neo4j
from data2neo import Converter

import warnings

def test_invalid_connection():
    warnings.filterwarnings("ignore", category=DeprecationWarning)
    with pytest.raises(ValueError) as excinfo:
        Converter("dummypath", None, "bolt://invalidlink:7687", ("neo4j", "password"))

    exception_msg = excinfo.value.args[0]
    assert "Cannot resolve address" in exception_msg

    with pytest.raises(neo4j.exceptions.ServiceUnavailable) as excinfo:
        Converter("dummypath", None, "bolt://localhost:1111", ("neo4j", "password"))

def test_invalid_auth():
    warnings.filterwarnings("ignore", category=DeprecationWarning)
    with pytest.raises(neo4j.exceptions.AuthError) as excinfo:
        Converter("dummypath", None, "bolt://localhost:7687", ("neo4j", "wrongpassword"))

    exception_msg = excinfo.value.args[0]
    
    assert "The client is unauthorized due to authentication failure" in exception_msg


def test_deprecated():
    with pytest.raises(DeprecationWarning) as excinfo:
        Converter("RELATION()", None, "bolt://localhost:7687", ("neo4j", "password"))

    exception_msg = excinfo.value.args[0]
    assert "The RELATION keyword is deprecated. Please use RELATIONSHIP instead." in exception_msg