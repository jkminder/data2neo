
#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Extension module for py2neo. Contains a version of the py2neo Graph that allows for parallel relationships
with the same type. 

authors: Julian Minder
"""

from py2neo.cypher.queries import (unwind_create_relationships_query, 
                                unwind_create_nodes_query, 
                                unwind_merge_nodes_query,
                                unwind_merge_relationships_query,
                                cypher_join)
from py2neo import UniquenessError, Graph, Subgraph

class _SubgraphWithParallelRelations(Subgraph):
    def __db_create__(self, tx):
        """ Updated create function that allows for creation of parallel relationships.
        Create new data in a remote :class:`.Graph` from this
        :class:`.Subgraph`.
        :param tx:
        """
        graph = tx.graph

        # Convert nodes into a dictionary of
        #   {frozenset(labels): [Node, Node, ...]}
        node_dict = {}
        for node in self.nodes:
            if not self._is_bound(node, tx.graph):
                key = frozenset(node.labels)
                node_dict.setdefault(key, []).append(node)

        # Convert relationships into a dictionary of
        #   {rel_type: [Rel, Rel, ...]}
        rel_dict = {}
        for relationship in self.relationships:
            if not self._is_bound(relationship, tx.graph):
                key = type(relationship).__name__
                rel_dict.setdefault(key, []).append(relationship)

        for labels, nodes in node_dict.items():
            pq = unwind_create_nodes_query(list(map(dict, nodes)), labels=labels)
            pq = cypher_join(pq, "RETURN id(_)")
            records = tx.run(*pq)
            for i, record in enumerate(records):
                node = nodes[i]
                node.graph = graph
                node.identity = record[0]
                node._remote_labels = labels
        for r_type, relationships in rel_dict.items():
            data = map(lambda r: [r.start_node.identity, dict(r), r.end_node.identity],
                        relationships)
            #### CHANGED from merge to create ####
            pq = unwind_create_relationships_query(data, r_type)
            ######################################
            pq = cypher_join(pq, "RETURN id(_)")
            for i, record in enumerate(tx.run(*pq)):
                relationship = relationships[i]
                relationship.graph = graph
                relationship.identity = record[0]

    def __db_merge__(self, tx, primary_label=None, primary_key=None):
        """ Updated create function that allows for merging of parallel relationships.
        Merge data into a remote :class:`.Graph` from this
        :class:`.Subgraph`.
        :param tx:
        :param primary_label:
        :param primary_key:
        """
        graph = tx.graph

        # Convert nodes into a dictionary of
        #   {(p_label, p_key, frozenset(labels)): [Node, Node, ...]}
        node_dict = {}
        for node in self.nodes:
            if not self._is_bound(node, graph):
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
                elif node.__model__ is not None:
                    p_key = node.__model__.__primarykey__ or primary_key
                else:
                    p_key = primary_key
                # Add node to the node dictionary
                key = (p_label, p_key, frozenset(node.labels))
                node_dict.setdefault(key, []).append(node)

        # Convert relationships into a dictionary of
        #   {rel_type: [Rel, Rel, ...]}
        rel_dict = {}
        for relationship in self.relationships:
            if not self._is_bound(relationship, graph):
                # Determine primary key
                if getattr(relationship, "__primarykey__", None) is not None:
                    p_key = relationship.__primarykey__
                else:
                    p_key = primary_key
                key = (p_key, type(relationship).__name__)
                rel_dict.setdefault(key, []).append(relationship)

        for (pl, pk, labels), nodes in node_dict.items():
            if pl is None or pk is None:
                raise ValueError("Primary label and primary key are required for node MERGE operation")
            pq = unwind_merge_nodes_query(map(dict, nodes), (pl, pk), labels)
            pq = cypher_join(pq, "RETURN id(_)")
            identities = [record[0] for record in tx.run(*pq)]
            if len(identities) > len(nodes):
                raise UniquenessError("Found %d matching nodes for primary label %r and primary "
                                        "key %r with labels %r but merging requires no more than "
                                        "one" % (len(identities), pl, pk, set(labels)))
            for i, identity in enumerate(identities):
                node = nodes[i]
                node.graph = graph
                node.identity = identity
                node._remote_labels = labels
        for (pk, r_type), relationships in rel_dict.items():
            if pk is None:
                raise ValueError("Primary key are required for relationship MERGE operation")
            data = map(lambda r: [r.start_node.identity, dict(r), r.end_node.identity],
                        relationships)
            pq = unwind_merge_relationships_query(data, (r_type, pk))
            pq = cypher_join(pq, "RETURN id(_)")
            identities = [record[0] for record in tx.run(*pq)]
            if len(identities) > len(relationships):
                raise UniquenessError("Found %d matching relations for primary "
                                        "key %r with type %r but merging requires no more than "
                                        "one" % (len(identities), pk, r_type))
            for i, identity in enumerate(identities):
                relationship = relationships[i]
                relationship.graph = graph
                relationship.identity = identity

class GraphWithParallelRelations(Graph):
    """
    This wrapper modifies the functionality of the py2neo Graph. This is mainly required due to a
    design decision of the py2neo library, that relations are unique between two nodes. This is not what we desire.
    This wrapper adapts the functionality of the py2neo graph to support multiple parallel relations 
    of the same type between two nodes.

    Only ::create and ::merge are tested. All other functionalities of the graph are not guaranteed.
    """
    def __init__(self, profile=None, name=None, **settings):
        super().__init__(profile=profile, name=name, **settings)

    def create(self, subgraph: Subgraph):
        subgraph = _SubgraphWithParallelRelations(subgraph.nodes, subgraph.relationships) # Replace create script
        return super().create(subgraph)

    def merge(self, subgraph: Subgraph, label=None, *property_keys):
        subgraph = _SubgraphWithParallelRelations(subgraph.nodes, subgraph.relationships) # Replace create script
        return super().merge(subgraph, label, *property_keys)