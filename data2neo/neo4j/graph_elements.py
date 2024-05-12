#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2023, Nigel Small and Julian Minder
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
Adapted from the now EOL py2neo: https://github.com/py2neo-org/py2neo

Elements that represents any entity in a Neo4j graph.

Inheritance Structure:

    GraphElement
        |- Attribute
        |- Subgraph
            |- Node
            |- Relationship

TODO: If you want to change the underlying driver, be sure to update the classes Subgraph, Node, Relation and NodeMatcher.

authors: Julian Minder and Nigel Small
"""

from abc import ABC
from typing import Any, List, Union
from datetime import datetime,date
import numbers
from itertools import chain
from .cypher import unwind_create_nodes_query, \
                    unwind_merge_nodes_query, \
                    unwind_create_relationships_query, \
                    unwind_merge_relationships_query, \
                    cypher_join, cypher_escape
from .encoder import encode_value, xstr, is_safe_key
from collections import OrderedDict

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
        if not self._value is None and not isinstance(self._value, (numbers.Number, str, bool, date, datetime)):
            return str(self._value)
        return self._value


class _GhostPrimaryKey:
    """
    A class that represents the presence of a primary key without any value, this is used to 
    merge relations that have no primary key.
    """
    pass

class Subgraph(GraphElement):
    """ 
    A :class:`.Subgraph` is an arbitrary collection of nodes and
    relationships. It is also the base class for :class:`.Node`,
    :class:`.Relationship` and :class:`.Path`.

    By definition, a subgraph must contain at least one node;
    `null subgraphs <http://mathworld.wolfram.com/NullGraph.html>`_
    should be represented by :const:`None`. To test for
    `emptiness <http://mathworld.wolfram.com/EmptyGraph.html>`_ the
    built-in :func:`bool` function can be used.

    The simplest way to construct a subgraph is by combining nodes and
    relationships using standard set operations. For example::

        >>> s = ab | ac
        >>> s
        {(alice:Person {name:"Alice"}),
         (bob:Person {name:"Bob"}),
         (carol:Person {name:"Carol"}),
         (Alice)-[:KNOWS]->(Bob),
         (Alice)-[:WORKS_WITH]->(Carol)}
        >>> s.nodes()
        frozenset({(alice:Person {name:"Alice"}),
                   (bob:Person {name:"Bob"}),
                   (carol:Person {name:"Carol"})})
        >>> s.relationships()
        frozenset({(Alice)-[:KNOWS]->(Bob),
                   (Alice)-[:WORKS_WITH]->(Carol)})

    .. describe:: subgraph | other | ...

        Union.
        Return a new subgraph containing all nodes and relationships from *subgraph* as well as all those from *other*.
        Any entities common to both will only be included once.

    .. describe:: subgraph & other & ...

        Intersection.
        Return a new subgraph containing all nodes and relationships common to both *subgraph* and *other*.

    .. describe:: subgraph - other - ...

        Difference.
        Return a new subgraph containing all nodes and relationships that exist in *subgraph* but do not exist in *other*,
        as well as all nodes that are connected by the relationships in *subgraph* regardless of whether or not they exist in *other*.

    .. describe:: subgraph ^ other ^ ...

        Symmetric difference.
        Return a new subgraph containing all nodes and relationships that exist in *subgraph* or *other*, but not in both,
        as well as all nodes that are connected by those relationships regardless of whether or not they are common to *subgraph* and *other*.

    """
    def __init__(self, nodes=None, relationships=None):
        self.__nodes = frozenset(nodes or [])
        self.__relationships = frozenset(relationships or [])
        self.__nodes |= frozenset(chain.from_iterable(r.nodes for r in self.__relationships))
        #if not self.__nodes:
        #    raise ValueError("Subgraphs must contain at least one node")

    def __repr__(self):
        return "Subgraph({%s}, {%s})" % (", ".join(map(repr, self.nodes)),
                                         ", ".join(map(repr, self.relationships)))


    def __db_create__(self, tx):
        """ Create new data in a remote :class:`.Graph` from this
        :class:`.Subgraph`.

        :param tx:
        """

        # Convert nodes into a dictionary of
        #   {frozenset(labels): [Node, Node, ...]}
        node_dict = {}
        for node in self.nodes:
            if node.identity is None:
                key = frozenset(node.labels)
                node_dict.setdefault(key, []).append(node)

        # Convert relationships into a dictionary of
        #   {rel_type: [Rel, Rel, ...]}
        rel_dict = {}
        for relationship in self.relationships:
            if relationship.identity is None:
                key = relationship.type
                rel_dict.setdefault(key, []).append(relationship)

        for labels, nodes in node_dict.items():
            pq = unwind_create_nodes_query(list(map(dict, nodes)), labels=labels)
            # TODO: id() is deprecated, in the future we need to move to something else
            pq = cypher_join(pq, "RETURN id(_)")
            records = tx.run(*pq)

            for i, record in enumerate(records):
                node = nodes[i]
                node.identity = record[0]
                node._remote_labels = frozenset(labels)
        for r_type, relationships in rel_dict.items():
            data = map(lambda r: [r.start_node.identity, dict(r), r.end_node.identity],
                       relationships)
            pq = unwind_create_relationships_query(data, r_type)
            # TODO: id() is deprecated, in the future we need to move to something else
            pq = cypher_join(pq, "RETURN id(_)")

            for i, record in enumerate(tx.run(*pq)):
                relationship = relationships[i]
                relationship.identity = record[0]

    def __db_merge__(self, tx, primary_label=None, primary_key=None):
        """ Merge data into a remote :class:`.Graph` from this
        :class:`.Subgraph`.

        :param tx:
        :param primary_label:
        :param primary_key:
        """

        # Convert nodes into a dictionary of
        #   {(p_label, p_key, frozenset(labels)): [Node, Node, ...]}
        node_dict = {}
        for node in self.nodes:
            if node.identity is None:
                # Determine primary label
                if node.__primarylabel__ is not None:
                    p_label = node.__primarylabel__
                elif node.__model__ is not None:
                    p_label = node.__model__.__primarylabel__ or primary_label
                else:
                    p_label = primary_label
                # Determine primary key
                if node.__primarykey__ is not None:
                    p_key = node.__primarykey__
                else:
                    p_key = primary_key
                
                # Add node to the node dictionary
                key = (p_label, p_key, frozenset(node.labels))
                node_dict.setdefault(key, []).append(node)

        # Convert relationships into a dictionary of
        #   {rel_type: [Rel, Rel, ...]}
        rel_dict = {}
        for relationship in self.relationships:
            if relationship.identity is None:
                # Determine primary key
                if getattr(relationship, "__primarykey__", None) is not None:
                    p_key = relationship.__primarykey__
                else:
                    p_key = primary_key
                key = (p_key, relationship.type)
                rel_dict.setdefault(key, []).append(relationship)

        for (pl, pk, labels), nodes in node_dict.items():
            if pl is None or pk is None:
                raise ValueError("Primary label and primary key are required for node MERGE operation")
            pq = unwind_merge_nodes_query(map(dict, nodes), (pl, pk), labels)
             # TODO: id() is deprecated, in the future we need to move to something else
            pq = cypher_join(pq, "RETURN id(_)")
            identities = [record[0] for record in tx.run(*pq)]
            if len(identities) > len(nodes):
                raise ValueError("Found %d matching nodes for primary label %r and primary "
                                        "key %r with labels %r but merging requires no more than "
                                        "one" % (len(identities), pl, pk, set(labels)))
            for i, identity in enumerate(identities):
                node = nodes[i]
                node.identity = identity
                node._remote_labels = frozenset(labels)
        for (pk, r_type), relationships in rel_dict.items():
            if pk is None:
                raise ValueError("Primary key are required for relationship MERGE operation")
            data = map(lambda r: [r.start_node.identity, dict(r), r.end_node.identity],
                        relationships)
            if isinstance(pk, _GhostPrimaryKey):
                pq = unwind_merge_relationships_query(data, r_type)
            else:
                pq = unwind_merge_relationships_query(data, (r_type, pk))
            # TODO: id() is deprecated, in the future we need to move to something else
            pq = cypher_join(pq, "RETURN id(_)")
            identities = [record[0] for record in tx.run(*pq)]
            if len(identities) > len(relationships):
                raise ValueError("Found %d matching relations for primary "
                                        "key %r with type %r but merging requires no more than "
                                        "one" % (len(identities), pk, r_type))
            for i, identity in enumerate(identities):
                relationship = relationships[i]
                relationship.identity = identity

    def __db_pull__(self, tx):
        """ Copy data from a remote :class:`.Graph` into this
        :class:`.Subgraph`.

        :param tx:
        """
        # Pull nodes
        nodes = {}
        for node in self.nodes:
            if node.identity is not None:
                nodes[node.identity] = node
        query = tx.run("MATCH (_) WHERE id(_) in $x "
                       "RETURN id(_), labels(_), properties(_)", x=list(nodes.keys()))
        for identity, new_labels, new_properties in query:
            node = nodes[identity]
            node.labels = set(new_labels)
            node.properties = new_properties

        # Pull relationships
        relationships = {}
        for relationship in self.relationships:
            if relationship.identity is not None:
                relationships[relationship.identity] = relationship
        query = tx.run("MATCH ()-[_]->() WHERE id(_) in $x "
                       "RETURN id(_), properties(_)", x=list(relationships.keys()))
        for identity, new_properties in query:
            relationship = relationships[identity]
            relationship.properties = new_properties

    def __db_push__(self, tx):
        """ Copy data into a remote :class:`.Graph` from this
        :class:`.Subgraph`.

        :param tx:
        """
        for node in self.nodes:
            if node.identity is not None:
                clauses = ["MATCH (_) WHERE id(_) = $x", "SET _ = $y"]
                parameters = {"x": node.identity, "y": dict(node)}
                old_labels = node._remote_labels - node.labels
                if old_labels:
                    clauses.append("REMOVE _:%s" % ":".join(map(cypher_escape, old_labels)))
                new_labels = node.labels - node._remote_labels
                if new_labels:
                    clauses.append("SET _:%s" % ":".join(map(cypher_escape, new_labels)))
                tx.run("\n".join(clauses), parameters)
                node._remote_labels = node.labels
        for relationship in self.relationships:
            if relationship.identity is not None:
                clauses = ["MATCH ()-[_]->() WHERE id(_) = $x", "SET _ = $y"]
                parameters = {"x": relationship.identity, "y": dict(relationship)}
                tx.run("\n".join(clauses), parameters)

    @property
    def nodes(self):
        """ The set of all nodes in this subgraph.
        """
        return tuple(self.__nodes)

    @property
    def relationships(self):
        """ The set of all relationships in this subgraph.
        """
        return tuple(self.__relationships)

    def __iter__(self):
        return iter(self.__relationships)

    def __bool__(self):
        return bool(self.__relationships)

    def __nonzero__(self):
        return bool(self.__relationships)

    def __or__(self, other):
        return Subgraph(set(self.nodes) | set(other.nodes), set(self.relationships) | set(other.relationships))

    def __and__(self, other):
        return Subgraph(set(self.nodes) & set(other.nodes), set(self.relationships) & set(other.relationships))

    def __sub__(self, other):
        r = set(self.relationships) - set(other.relationships)
        n = (set(self.nodes) - set(other.nodes)) | set().union(*(set(rel.nodes) for rel in r))
        return Subgraph(n, r)

    def __xor__(self, other):
        r = set(self.relationships) ^ set(other.relationships)
        n = (set(self.nodes) ^ set(other.nodes)) | set().union(*(set(rel.nodes) for rel in r))
        return Subgraph(n, r)


class PropertyDict:
    """Abstract PropertyDict class that represents a dictionary of properties."""
    def __init__(self, **properties):
        self.properties = properties
        self._identity = None
        self.__primarykey__ = None

    @property
    def identity(self):
        """Identity of element"""
        return self._identity
    
    @identity.setter
    def identity(self, value):
        self._identity = value

    def keys(self):
        """Returns properties keys"""
        return self.properties.keys()
    
    def update(self, properties):
        """Updates the properties of the element"""
        self.properties.update(properties)
        
    def __getitem__(self, key):
        if key not in self.properties:
            return None
        return self.properties[key]
    
    def __setitem__(self, key, value):
        self.properties[key] = value

    def __delitem__(self, key):
        del self.properties[key]

    def __contains__(self, key):
        return key in self.properties
    
    def __iter__(self):
        return iter(self.properties)
    
    def __len__(self):
        return len(self.properties)

    def keys(self):
        """Returns properties keys"""
        return self.properties.keys()

    def set_primary_key(self, key):
        """Sets the primary key of the element"""
        if key not in self.properties.keys() and not isinstance(key, _GhostPrimaryKey):
            raise ValueError("Primary key must be one of the node properties")
        self.__primarykey__ = key

    def __getstate__(self):
        return self.__dict__
    
    def __hash__(self):
        try:
            if self._identity is not None:
                return hash(self._identity)
        except:
            pass
        return hash(id(self))
        

class Node(PropertyDict, Subgraph):
    """ A node is a fundamental unit of data storage within a property
    graph that may optionally be connected, via relationships, to
    other nodes.

    It possible to combine nodes (along with relationships and other
    graph data objects) into :class:`.Subgraph` objects using set
    operations. For more details, look at the documentation for the
    :class:`.Subgraph` class.

    All positional arguments passed to the constructor are interpreted
    as labels and all keyword arguments as properties::

        >>> from data2neo.neo4j import Node
        >>> a = Node("Person", name="Alice")

    """

    @staticmethod
    def from_attributes(labels: List[Attribute], attributes: List[Attribute] = [], primary_key: str = None, primary_label: str = None):
        """Creates a Node from a list of attributes and labels
        
        Args:
            labels: List of static attributes specifying the labels of the Node (first label will be the primary label)
            attributes: List of attributes (only one can be primary)
            primary_key: Optional key of the primary attribute. Used to merge the Node with existing nodes in the graph (default: None)
            primary_label: Optional label of the primary attribute. Used to merge the Node with existing nodes in the graph (default: None)
        """
        labels = [label.value for label in labels]
        properties = dict((attr.key, attr.value) for attr in attributes)
        node = Node(*labels, **properties)
        if primary_key:
            node.set_primary_key(primary_key)
        if primary_label:
            node.set_primary_label(primary_label)
        return node

    @staticmethod
    def from_dict(labels: List[str], properties: dict, primary_key: str = None, primary_label: str = None, identity: str = None):
        """Creates a Node from a list of attributes and labels
        
        Args:
            labels: List of static attributes specifying the labels of the Node (first label will be the primary label)
            properties: Dictionary of attributes (only one can be primary)
            primary_key: Optional key of the primary attribute. Used to merge the Node with existing nodes in the graph (default: None)
            primary_label: Optional label of the primary attribute. Used to merge the Node with existing nodes in the graph (default: None)
        """
        node = Node(*labels, **properties)
        if primary_key:
            node.set_primary_key(primary_key)
        if primary_label:
            node.set_primary_label(primary_label)
        if identity is not None:
            node.identity = identity
        return node

    def __init__(self, *labels: str, **attributes: str) -> None:
        """Inits a Node with labels and attributes
        
        Args:
            labels: List of static attributes specifying the labels of the Node (first label will be the primary label)
            attributes: Key value pairs of attributes for the Node
        """
        self.labels = set(labels)
        self._remote_labels = frozenset()
        self.__primarylabel__ = labels[0]
        
        PropertyDict.__init__(self, **attributes)
        Subgraph.__init__(self, nodes=[self])

    def set_primary_label(self, label):
        """Sets the primary label of the node
        
        Args:
            label: Label of the primary attribute. Used to merge the Node with existing nodes in the graph
        """
        if label not in self.labels:
            raise ValueError("Primary label must be one of the node labels")
        self.__primarylabel__ = label
        
    def __repr__(self):
        args = list(self.labels)
        kwargs = OrderedDict()
        d = dict(self)
        if self.identity is None:
            d["identity"] = self.identity
        for key in sorted(d):
            if is_safe_key(key):
                args.append("%s=%r" % (key, d[key]))
            else:
                kwargs[key] = d[key]
        if kwargs:
            args.append("**{%s}" % ", ".join("%r: %r" % (k, kwargs[k]) for k in kwargs))
        return "Node(%s)" % ", ".join(args)

    def __str__(self):
        return repr(self)

    def __eq__(self, other):
        if self is other:
            return True
        try:
            if any(x is None for x in [self.identity, other.identity]):
                return False
            return (issubclass(type(self), Node) and issubclass(type(other), Node) and
                     self.identity == other.identity)
        except (AttributeError, TypeError):
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return super().__hash__()


class Relationship(PropertyDict, Subgraph):
    @staticmethod
    def from_attributes(start_node: Node, relationship_type: Attribute, end_node: Node, attributes: List[Attribute] = [], primary_key: str = None):
        """Creates a Relationship from a list of attributes and labels
        
        Args:
            start_node: Origin of the relationship
            relationship_type: Type of the relationship
            end_node: Destination of the relationship
            attributes: List of attributes for the relationship
            primary_key: Optional key of the primary attribute. Used to merge the Relationship with existing relationships in the graph (default: None)
        """
        properties = dict((attr.key, attr.value) for attr in attributes)
        relationship = Relationship(start_node, relationship_type.value, end_node, **properties)
        if primary_key:
            relationship.set_primary_key(primary_key)
        return relationship
    
    @staticmethod
    def from_dict(start_node: Node, end_node: Node, relationship_type: str, properties: dict, primary_key: str = None, identity: str = None):
        """Creates a Relationship from a list of attributes and labels
        
        Args:
            start_node: Origin of the relationship
            end_node: Destination of the relationship
            relationship_type: Type of the relationship
            properties: Dictionary of attributes for the relationship
            primary_key: Optional key of the primary attribute. Used to merge the relationship with existing relations in the graph (default: None)
        """
        relationship = Relationship(start_node, relationship_type, end_node, **properties)
        if primary_key:
            relationship.set_primary_key(primary_key)
        if identity is not None:
            relationship.identity = identity
        return relationship

    def __init__(self, start_node: Node, relationship_type: str, end_node: Node, **attributes) -> None:
        """ A relationship represents a typed connection between a pair of nodes.

        The positional arguments passed to the constructor identify the nodes to
        relate and the type of the relationship. Keyword arguments describe the
        properties of the relationship::

            >>> from py2neo import Node, Relationship
            >>> a = Node("Person", name="Alice")
            >>> b = Node("Person", name="Bob")
            >>> a_knows_b = Relationship(a, "KNOWS", b, since=1999)

        Args:
            start_node: Origin of the relationship
            end_node: Destination of the relationship
            relationship_type: Type of the relationship
            attributes: Key value pairs of attributes for the Relationship
        """
        self._type = relationship_type 
        
        self._start_node = start_node
        self._end_node = end_node

        PropertyDict.__init__(self, **attributes)
        Subgraph.__init__(self, nodes=[start_node, end_node], relationships=[self])

    @property
    def start_node(self):
        """Start node of the relationship"""
        return self._start_node
    
    @property
    def end_node(self):
        """End node of the relationship"""
        return self._end_node
    
    @property
    def type(self):
        """Type of the relationship"""
        return self._type
    
    def __repr__(self):
        args = [repr(self.start_node), self.type, repr(self.end_node)]
        kwargs = OrderedDict()
        d = self.properties
        for key in sorted(d):
            if is_safe_key(key):
                args.append("%s=%r" % (key, d[key]))
            else:
                kwargs[key] = d[key]
        if kwargs:
            args.append("**{%s}" % ", ".join("%r: %r" % (k, kwargs[k]) for k in kwargs))
        return "%s(%s)" % (self.__class__.__name__, ", ".join(args))

    def __str__(self):
        return repr(self)

    def __eq__(self, other):
        if self is other:
            return True
        try:
            if any(x is None for x in [self.identity, other.identity]):
                try:
                    return self.type == other.type  and list(self.nodes) == list(other.nodes) and dict(self) == dict(other) and id(self) == id(other)
                except (AttributeError, TypeError):
                    return False
            return issubclass(type(self), Relationship) and issubclass(type(other), Relationship) and self.identity == other.identity
        except (AttributeError, TypeError):
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return super().__hash__()
