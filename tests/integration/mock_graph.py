#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Py2neo mock modules, that emulate the behavior of a neo4j database. Used for testing.

authors: Julian Minder
"""
from threading import Lock

class MockNodeResult:
    def __init__(self, set) -> None:
        self.set = set
    
    def all(self):
        return self.set
    
class MockNodeMatcher:
    def __init__(self, graph) -> None:
        self.graph = graph

    def match(self, *labels, **conditions):
        res = set()
        labels = set(labels)
        for node in self.graph.nodes:
            if node.labels.isdisjoint(labels):
                continue
            match = True
            for (key, value) in conditions.items():
                if node[key] != value:
                    match = False
                    break
            if match:
                res.add(node)
        return MockNodeResult(res)

class MockGraph:
    """A simple py2neo graph immulating class"""
    current_node_id = 0 # used to identify nodes

    def __init__(self) -> None:
        self.nodes = []
        self.relations = []
        self.matcher = MockNodeMatcher(self)
        self.nodes_lock = Lock()
        self.relations_lock = Lock()

        # Dummy variables to simulate graph
        self.service = object() # Dummy service
        self.name = "MockGraph"
        self._allow_parallel_relations = False
        
    @property
    def allow_parallel_relations(self):
        return self._allow_parallel_relations

    @allow_parallel_relations.setter
    def allow_parallel_relations(self, value):
        self._allow_parallel_relations = value
    
    def delete_all(self):
        self.nodes = []
        self.relations = []

    def create_node(self, node):
        node.identity = MockGraph.current_node_id
        MockGraph.current_node_id += 1
        with self.nodes_lock:
            self.nodes.append(node)

    def create_relation(self, relation):
        relation.graph = self
        with self.relations_lock:
            self.relations.append(relation)

    def _merge_relation(self, relation):
        found = False   
        for rel in self.relations:
            if relation.start_node.identity == rel.start_node.identity and \
                relation.end_node.identity == rel.end_node.identity and \
                    type(relation).__name__ == type(rel).__name__:
                found = True
                # update relation
                for key in relation.keys():
                    rel[key] = relation[key]
        if not found:
            self.create_relation(relation)

    def create(self, subgraph):
        for node in subgraph.nodes:
            if node.identity is None:
                self.create_node(node)
        for relation in subgraph.relationships:
            if relation.graph is None:
                if not self._allow_parallel_relations:
                    self._merge_relation(relation)
                else:
                    self.create_relation(relation)

    def merge(self, subgraph):
        for relation in subgraph.relationships:
            self._merge_relation(relation)

        for node in subgraph.nodes:
            old_node = None
            if node.identity is not None:
                for n in self.nodes:
                    if n.identity == node.identity:
                        old_node = n
                        break

            if old_node is None:    
                match = self.matcher.match(node.__primarylabel__, **{node.__primarykey__: node[node.__primarykey__]})
                if len(match.all()) == 0:
                    self.create_node(node)
                    continue
                elif len(match.all()) > 1:
                    raise ValueError("Multiple nodes found to merge")
                else:
                    old_node = min(match.all())

            # update old node
            old_node.update_labels(node.labels)
            for key in node.keys():
                old_node[key] = node[key]
            node.identity = old_node.identity
    
    def __repr__(self) -> str:
        return f"MockGraph({self.nodes},\n {self.relations})"