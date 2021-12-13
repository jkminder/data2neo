#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Subgraph utilities

authors: Julian Minder
"""
import warnings
from .. import register_subgraph_postprocessor
from .. import Matcher

@register_subgraph_postprocessor
def UNIQUE(node):
    """This will grab an existing node from the graph if the constructed one already exists.
    Keeps this node unique"""
    matcher = Matcher.graph_matcher
    match = matcher.match(*list(node.labels), **dict(node)).all()
    assert len(match) <= 1, "Node specified with UNIQUE is not unique in the given graph"
    if len(match) == 1:
        return match[0]
    else:
        return node