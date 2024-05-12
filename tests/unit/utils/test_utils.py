#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tests for data2neo.utils module

authors: Julian Minder
"""

import pytest

from data2neo.utils import load_file

def test_load_file():
    """
    Tests the load_file function.
    """
    file_path = "tests/unit/utils/resources/testfile.yaml"
    result = load_file(file_path)
    assert result == "This is a test file.\nAnother line."

