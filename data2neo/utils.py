#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Utility functions for working with data2neo.

authors: Julian Minder
"""


def load_file(file_path: str) -> str:
    """
    Loads a file and returns its contents.
    """
    with open(file_path, "r", encoding="utf-8") as fstream:
        return fstream.read()