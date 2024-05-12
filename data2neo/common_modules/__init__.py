#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module containing common pre and postprocessing modules

Import this module to register them in your program.

authors: Julian Minder
"""

# For legacy reasons, we need to import the modules here
from .datetime import DATE, DATETIME
from .. import register_subgraph_postprocessor
from ..neo4j.graph_elements import _GhostPrimaryKey

@register_subgraph_postprocessor
def MERGE_RELATIONSHIPS(subgraph):
    """
    Subgraph postprocessor that merges relationships between the same two nodes. Only applicable if a GraphWithParallelRelations is used (otherwise this is the default) and 
    if the relationship has no primary key.
    """
    for relationships in subgraph.relationships:
        if getattr(relationships, "__primarykey__", None) is None:
            relationships.set_primary_key(_GhostPrimaryKey())
    return subgraph


@register_subgraph_postprocessor
def MERGE_RELATIONS(subgraph):
    """Deprecated, use MERGE_RELATIONSHIPS instead"""
    raise DeprecationWarning("MERGE_RELATIONS is deprecated, use MERGE_RELATIONSHIPS instead")
    return MERGE_RELATIONSHIPS(subgraph)