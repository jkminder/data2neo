#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Elements that represents any entity in a Neo4j graph or interactions with the graph. Subgraph, Node and Relation are abstractions of the py2neo node and relation.
This allows us to customize their functionality and one could easily exchange the neo4j driver. 

Inheritance Structure:

    GraphElement
        |- Attribute
        |- Subgraph
            |- Node
            |- Relation

TODO: If you want to change the underlying driver, be sure to update the classes Subgraph, Node, Relation and NodeMatcher.

authors: Julian Minder
"""

from abc import ABC
from typing import List, Union
from datetime import datetime,date
import numbers
import py2neo


class GraphElement(ABC):
    """Abstract GraphElementType
    
    Represents attributes, subgraphs, nodes and relations. 
    """
    pass

class Attribute:
    """Represents an attribute in an Node or in an Relation.
    
    Attributes:
        key: String signifying the key of the attribute
        value: Can be any value that is allowed in the graph 
    """

    def __init__(self, key: str, value: Union[str, int, float, bool, datetime]) -> None:
        """Inits an attribute with a key and a value
        
        Args:
            key: String signifying the key of the attribute
            value: Can be any value that is allowed in the graph (String, Int, Float, Bool)
        """
        super().__init__()
        self._key = key
        self._value = value

    @property
    def key(self):
        """String signifying the key of the attribute"""
        return self._key

    @property
    def value(self):
        """Any value that is allowed in the graph (String, Int, Float, Bool)"""
        # If the value is not allowed in neo4j it is converted to strings
        if not isinstance(self._value, (numbers.Number, str, bool, date, datetime)):
            return str(self._value)
        return self._value



class Subgraph(py2neo.Subgraph,  GraphElement):
    """Subgraph Abstraction of py2neo.Subgraph"""
    pass



class Node(py2neo.Node, Subgraph):
    """Node Abstraction of py2neo.Node"""

    def __init__(self, labels: List[Attribute], attributes: List[Attribute], primary_key: str = None) -> None:
        """Inits a Node with labels and attributes
        
        Args:
            labels: List of static attributes specifying the labels of the Node (first label will be the primary label)
            attributes: List of attributes (only one can be primary)
            primary_key: Optional key of the primary attribute. Used to merge the Node with existing nodes in the graph (default: None)
        """
        super().__init__(*[label.value for label in labels], **dict((attr.key, attr.value) for attr in attributes))
        self.__primarylabel__ = labels[0].value
        self.__primarykey__ = primary_key



class Relation(py2neo.Relationship, Subgraph):
    """Relation Abstraction of py2neo.Relationship"""

    def __init__(self, from_node: Node, type: str, to_node: Node, attributes: List[Attribute]) -> None:
        """Inits a Relation with a origin, a type, a destination and attributes

        Args:
            from_node: Origin of the relation
            to_node: Destination of the relation
            type: Type of the relation
            attributes: List of attributes for the relation
        """
        super().__init__(from_node, type.value, to_node, **dict((attr.key, attr.value) for attr in attributes))

class NodeMatcher(py2neo.NodeMatcher):
    """NodeMatcher Abstraction of py2neo.NodeMatcher"""
    pass

class Graph(py2neo.Graph):
    """Graph Abstraction of py2neo.Graph"""
    pass

