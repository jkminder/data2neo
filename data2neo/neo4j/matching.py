from neo4j import Session
from typing import List, Union

from .graph_elements import Node, Relationship, Subgraph, Attribute
from .cypher import cypher_join, _match_clause, encode_value, encode_key
from abc import ABC, abstractmethod

class ResultIterator(ABC):
    def __init__(self, count, match):
        self._count = count
        self._match = match
        
    def __len__(self):
        return self._count
    
    @abstractmethod
    def __iter__(self):
        pass
    
class NodeIterator(ResultIterator):
    def __iter__(self):
        for record in self._match:
            node = Node.from_dict(record['LABELS(n)'], record['n'], identity=record['ID(n)'])
            yield node
            
class RelationshipIterator(ResultIterator):
    def __iter__(self):
        for record in self._match:
            fn = Node.from_dict(record['LABELS(from_node)'], record['from_node'], identity=record['ID(from_node)'])
            tn = Node.from_dict(record['LABELS(to_node)'], record['to_node'], identity=record['ID(to_node)'])
            rel = Relationship.from_dict(fn, tn, record['TYPE(r)'], record['PROPERTIES(r)'], identity=record['ID(r)'])
            yield rel
    
def match_nodes(session: Session, *labels: List[str], return_iterator=False, **properties: dict):
    """
    Matches nodes in the database.

    Args:
        session (Session): The `session <https://neo4j.com/docs/api/python-driver/current/api.html#session>`_ to use.
        labels (List[str]): The labels to match.
        return_iterator (bool): Whether to return an iterator or a list (Default: False)
        properties (dict): The properties to match.
    """
    flat_params = [tuple(labels),]
    data = []
    for k, v in properties.items():
        data.append(v)
        flat_params.append(k)

    if len(data) > 1:
        data = [data]
        
    unwind = "UNWIND $data as r" if len(data) > 0 else ""
    
    
    clause = cypher_join(unwind, _match_clause('n', tuple(flat_params), "r"), "RETURN n, LABELS(n), ID(n)", data=data)
    count_clause = cypher_join(unwind, _match_clause('n', tuple(flat_params), "r"), "RETURN count(n)", data=data)

    count = session.run(*count_clause).single().value()
    
    match = session.run(*clause)
    iterator = NodeIterator(count, match)
    
    if return_iterator:
        return iterator
    else:
        return list(iterator)

def match_relationships(session: Session, from_node: Node =None, to_node:Node =None, rel_type: str =None, return_iterator=False, **properties: dict):
    """
    Matches relationships in the database.

    Args:
        session (Session): The `session <https://neo4j.com/docs/api/python-driver/current/api.html#session>`_ to use.
        from_node (Node): The node to match the relationship from (Default: None)
        to_node (Node): The node to match the relationship to (Default: None)
        rel_type (str): The type of the relationship to match (Default: None)
        return_iterator (bool): Whether to return an iterator or a list (Default: False)
        properties (dict): The properties to match.
    """
    if from_node is not None:
        assert from_node.identity is not None, "from_node must have an identity"

    if to_node is not None:
        assert to_node.identity is not None, "to_node must have an identity"

    params = ""
    for k, v in properties.items():
        if params != "":
            params += ", "
        params += f"{encode_key(k)}: {encode_value(v)}"

    clauses = []
    if from_node is not None:
        clauses.append(f"ID(from_node) = {from_node.identity}")
    if to_node is not None:
        clauses.append(f"ID(to_node) = {to_node.identity}")
    if rel_type is not None:
        clauses.append(f"type(r) = {encode_value(rel_type)}")

    clause = cypher_join(
        f"MATCH (from_node)-[r {{{params}}}]->(to_node)",
        "WHERE" if len(clauses) > 0 else "",
        " AND ".join(clauses),
        "RETURN PROPERTIES(r), TYPE(r), ID(r), from_node, LABELS(from_node), ID(from_node), to_node, LABELS(to_node), ID(to_node)"
    )
    count_clause = cypher_join(
        f"MATCH (from_node)-[r {{{params}}}]->(to_node)",
        "WHERE" if len(clauses) > 0 else "",
        " AND ".join(clauses),
        "RETURN count(r)"
    )
    count = session.run(*count_clause).single().value()
    
    match = session.run(*clause)
    
    if return_iterator:
        return RelationshipIterator(count, match)
    else:
        return list(RelationshipIterator(count, match))