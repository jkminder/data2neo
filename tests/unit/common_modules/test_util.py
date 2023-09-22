#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tests for common modules: util

authors: Julian Minder
"""

import pytest

from rel2graph.common_modules import datetime
from rel2graph import Attribute

from datetime import datetime as dt

@pytest.fixture
def default_format():
    return Attribute("key", "2015-05-17T21:18:19")

@pytest.fixture
def other_format():
    return Attribute("key", "2015/05/17 21h 18min 19s")

@pytest.fixture
def as_datetime():
    return Attribute("key", dt.strptime("2015-05-17T21:18:19", "%Y-%m-%dT%H:%M:%S"))

def test_date(default_format, other_format, as_datetime):
    result = datetime.DATE(default_format).value
    assert(result.year == 2015)
    assert(result.month == 5)
    assert(result.day == 17)
    result = datetime.DATE(other_format, "%Y/%m/%d %Hh %Mmin %Ss").value
    assert(result.year == 2015)
    assert(result.month == 5)
    assert(result.day == 17)
    result = datetime.DATE(as_datetime).value
    assert(result.year == 2015)
    assert(result.month == 5)
    assert(result.day == 17)

def test_datetime(default_format, other_format, as_datetime):
    result = datetime.DATETIME(default_format).value
    assert(result.year == 2015)
    assert(result.month == 5)
    assert(result.day == 17)
    assert(result.hour == 21)
    assert(result.minute == 18)
    assert(result.second == 19)
    result = datetime.DATETIME(other_format, "%Y/%m/%d %Hh %Mmin %Ss").value
    assert(result.year == 2015)
    assert(result.month == 5)
    assert(result.day == 17)
    assert(result.hour == 21)
    assert(result.minute == 18)
    assert(result.second == 19)    
    result = datetime.DATETIME(as_datetime).value
    assert(result.year == 2015)
    assert(result.month == 5)
    assert(result.day == 17)
    assert(result.hour == 21)
    assert(result.minute == 18)
    assert(result.second == 19)