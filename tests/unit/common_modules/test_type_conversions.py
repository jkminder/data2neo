#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tests for common modules: util

authors: Julian Minder
"""

import pytest

from rel2graph.common_modules import types
from rel2graph import Attribute

@pytest.fixture
def s_attr():
    return Attribute("key", "40")

@pytest.fixture
def other_format():
    return Attribute("key", "2015/05/17 21h 18min 19s")

@pytest.fixture
def as_datetime():
    return Attribute("key", datetime.strptime("2015-05-17T21:18:19", "%Y-%m-%dT%H:%M:%S"))

def test_int():
    result = types.INT(Attribute("key", "40")).value
    assert(result == 40)

    result = types.INT(Attribute("key", 5.3)).value
    assert(result == 5)

    result = types.INT(Attribute("key", 5)).value
    assert(result == 5)

def test_float():
    result = types.FLOAT(Attribute("key", "40")).value
    assert(result == 40.0)

    result = types.FLOAT(Attribute("key", 5.3)).value
    assert(result == 5.3)

    result = types.FLOAT(Attribute("key", 5)).value
    assert(result == 5.0)

def test_bool():
    result = types.BOOL(Attribute("key", "True")).value
    assert(result == True)

    result = types.BOOL(Attribute("key", "False")).value
    assert(result == True)

    result = types.BOOL(Attribute("key", True)).value
    assert(result == True)

    result = types.BOOL(Attribute("key", False)).value
    assert(result == False)

    result = types.BOOL(Attribute("key", None)).value
    assert(result == False)

    result = types.BOOL(Attribute("key", 0)).value
    assert(result == False)

    result = types.BOOL(Attribute("key", 1)).value
    assert(result == True)

def test_str():
    result = types.STR(Attribute("key", "40")).value
    assert(result == "40")

    result = types.STR(Attribute("key", 5.3)).value
    assert(result == "5.3")

    result = types.STR(Attribute("key", 5)).value
    assert(result == "5")

