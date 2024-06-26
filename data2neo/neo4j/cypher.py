#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2021, Nigel Small
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Common cypher queries. 
Adapted and copied from the now EOL py2neo: https://github.com/py2neo-org/py2neo

authors: Julian Minder and Nigel Small
"""

from collections import OrderedDict

from .encoder import encode_value, string_types, encode_key, CypherExpression



### Cypher modifiers

def cypher_join(*clauses, **parameters):
    """ Join multiple Cypher clauses, returning a (query, parameters)
    tuple. Each clause may either be a simple string query or a
    (query, parameters) tuple. Additional `parameters` may also be
    supplied as keyword arguments.

    :param clauses:
    :param parameters:
    :return: (query, parameters) tuple
    """
    query = []
    params = {}
    for clause in clauses:
        if clause is None:
            continue
        if isinstance(clause, tuple):
            try:
                q, p = clause
            except ValueError:
                raise ValueError("Expected query or (query, parameters) tuple "
                                 "for clause %r" % clause)
        else:
            q = clause
            p = None
        query.append(q)
        if p:
            params.update(p)
    params.update(parameters)
    return "\n".join(query), params

def cypher_escape(identifier):
    """ Return a Cypher identifier, with escaping if required.

    Simple Cypher identifiers, which just contain alphanumerics
    and underscores, can be represented as-is in expressions.
    Any which contain more esoteric characters, such as spaces
    or punctuation, must be escaped in backticks. Backticks
    themselves are escaped by doubling.

    ::

        >>> cypher_escape("simple_identifier")
        'simple_identifier'
        >>> cypher_escape("identifier with spaces")
        '`identifier with spaces`'
        >>> cypher_escape("identifier with `backticks`")
        '`identifier with ``backticks```'

    Identifiers are used in Cypher to denote named values, labels,
    relationship types and property keys. This function will typically
    be used to construct dynamic Cypher queries in places where
    parameters cannot be used.

        >>> "MATCH (a:{label}) RETURN id(a)".format(label=cypher_escape("Employee of the Month"))
        'MATCH (a:`Employee of the Month`) RETURN id(a)'

    :param identifier: any non-empty string
    """
    if not isinstance(identifier, string_types):
        raise TypeError(type(identifier).__name__)
    return encode_key(identifier)

    
#######

def unwind_create_nodes_query(data, labels=None, keys=None):
    """ Generate a parameterised ``UNWIND...CREATE`` query for bulk
    loading nodes into Neo4j.

    :param data:
    :param labels:
    :param keys:
    :return: (query, parameters) tuple
    """
    return cypher_join("UNWIND $data AS r",
                       _create_clause("_", (tuple(labels or ()),)),
                       _set_properties_clause("r", keys),
                       data=list(data))


def unwind_merge_nodes_query(data, merge_key, labels=None, keys=None, preserve=None):
    """ Generate a parameterised ``UNWIND...MERGE`` query for bulk
    loading nodes into Neo4j.

    :param data:
    :param merge_key:
    :param labels:
    :param keys:
    :param preserve:
        Collection of key names for values that should be protected
        should the node already exist.
    :return: (query, parameters) tuple
    """
    return cypher_join("UNWIND $data AS r",
                       _merge_clause("_", merge_key, "r", keys),
                       _on_create_set_properties_clause("r", keys, preserve),
                       _set_properties_clause("r", keys, exclude_keys=preserve),
                       _set_labels_clause(labels),
                       data=list(data))


def unwind_create_relationships_query(data, rel_type, start_node_key=None, end_node_key=None,
                                      keys=None):
    """ Generate a parameterised ``UNWIND...CREATE`` query for bulk
    loading relationships into Neo4j.

    :param data:
    :param rel_type:
    :param start_node_key:
    :param end_node_key:
    :param keys:
    :return: (query, parameters) tuple
    """
    return cypher_join("UNWIND $data AS r",
                       _match_clause("a", start_node_key, "r[0]"),
                       _match_clause("b", end_node_key, "r[2]"),
                       _create_clause("_", rel_type, "(a)-[", "]->(b)"),
                       _set_properties_clause("r[1]", keys),
                       data=_relationship_data(data))


def unwind_merge_relationships_query(data, merge_key, start_node_key=None, end_node_key=None,
                                     keys=None, preserve=None):
    """ Generate a parameterised ``UNWIND...MERGE`` query for bulk
    loading relationships into Neo4j.

    :param data:
    :param merge_key: tuple of (rel_type, key1, key2...)
    :param start_node_key:
    :param end_node_key:
    :param keys:
    :param preserve:
        Collection of key names for values that should be protected
        should the relationship already exist.
    :return: (query, parameters) tuple
    """
    return cypher_join("UNWIND $data AS r",
                       _match_clause("a", start_node_key, "r[0]"),
                       _match_clause("b", end_node_key, "r[2]"),
                       _merge_clause("_", merge_key, "r[1]", keys, "(a)-[", "]->(b)"),
                       _on_create_set_properties_clause("r[1]", keys, preserve),
                       _set_properties_clause("r[1]", keys, exclude_keys=preserve),
                       data=_relationship_data(data))


class NodeKey(object):

    def __init__(self, node_key):
        if isinstance(node_key, tuple):
            self.__pl = node_key[0]
            self.__pk = ()
            for pk in node_key[1:]:
                if isinstance(pk, tuple):
                    self.__pk += pk
                else:
                    self.__pk += (pk,)
        else:
            self.__pl, self.__pk = node_key, ()
        if not isinstance(self.__pl, tuple):
            self.__pl = (self.__pl or "",)

    def label_string(self):
        label_set = set(self.__pl)
        return "".join(":" + cypher_escape(label) for label in sorted(label_set))

    def keys(self):
        return self.__pk

    def key_value_string(self, value, ix):
        return ", ".join("%s:%s[%s]" % (cypher_escape(key), value, encode_value(ix[i]))
                         for i, key in enumerate(self.__pk))
    

def _create_clause(name, node_key, prefix="(", suffix=")"):
    return "CREATE %s%s%s%s" % (prefix, name, NodeKey(node_key).label_string(), suffix)

def _merge_clause(name, merge_key, value, keys, prefix="(", suffix=")"):
    nk = NodeKey(merge_key)
    merge_keys = nk.keys()
    if len(merge_keys) == 0:
        return "MERGE %s%s%s%s" % (
            prefix, name, nk.label_string(), suffix)
    elif keys is None:
        return "MERGE %s%s%s {%s}%s" % (
            prefix, name, nk.label_string(), nk.key_value_string(value, merge_keys), suffix)
    else:
        return "MERGE %s%s%s {%s}%s" % (
            prefix, name, nk.label_string(), nk.key_value_string(value, [keys.index(key) for key in
                                                                 merge_keys]), suffix)

def _match_clause(name, node_key, value, prefix="(", suffix=")"):
    if node_key is None:
        # ... add MATCH by id clause # TODO: id() is deprecated, in the future we need to move to something else
        #  -> find alternative
        return "MATCH %s%s%s WHERE id(%s) = %s" % (prefix, name, suffix, name, value)
    else:
        # ... add MATCH by label/property clause
        nk = NodeKey(node_key)
        n_pk = len(nk.keys())
        if n_pk == 0:
            return "MATCH %s%s%s%s" % (
                prefix, name, nk.label_string(), suffix)
        elif n_pk == 1:
            return "MATCH %s%s%s {%s:%s}%s" % (
                prefix, name, nk.label_string(), cypher_escape(nk.keys()[0]), value, suffix)
        else:
            return "MATCH %s%s%s {%s}%s" % (
                prefix, name, nk.label_string(), nk.key_value_string(value, list(range(n_pk))),
                suffix)
        
def _on_create_set_properties_clause(expr, all_keys, keys):
    if keys is None:
        return None
    else:
        # data is list of lists
        d = OrderedDict()
        for i, key in enumerate(all_keys):
            if key in keys:
                d[key] = CypherExpression("%s[%d]" % (expr, i))

                
        return "ON CREATE SET _ += " + encode_value(d)
    

def _set_labels_clause(labels):
    if labels:
        return "SET _%s" % NodeKey((tuple(labels),)).label_string()
    else:
        return None

def _set_properties_clause(expr, keys, exclude_keys=()):
    if keys is None:
        # data is list of dicts
        return "SET _ += %s" % expr
    else:
        # data is list of lists
        d = OrderedDict()
        for i, key in enumerate(keys):
            if exclude_keys and key in exclude_keys:
                continue
            d[key] = CypherExpression("%s[%d]" % (expr, i))
        return "SET _ += " + encode_value(d)
    

def _relationship_data(data):
    norm_data = []
    for item in data:
        start_node, detail, end_node = item
        norm_start = type(start_node) is tuple and len(start_node) == 1
        norm_end = type(end_node) is tuple and len(end_node) == 1
        if norm_start and norm_end:
            norm_data.append((start_node[0], detail, end_node[0]))
        elif norm_start:
            norm_data.append((start_node[0], detail, end_node))
        elif norm_end:
            norm_data.append((start_node, detail, end_node[0]))
        else:
            norm_data.append(item)
    return norm_data


