from neo4j import Session
from typing import List, Union

from .graph_elements import Node, Relationship, Subgraph, Attribute
from .cypher import cypher_join, _match_clause, encode_value, encode_key

def create(graph: Subgraph, session: Session):
    """
    Creates a graph in the database.

    Args:
        graph (Subgraph): The graph to create.
        session (Session): The `session <https://neo4j.com/docs/api/python-driver/current/api.html#session>`_ to use.
    """
    session.execute_write(graph.__db_create__)

def merge(graph: Subgraph, session: Session, primary_label=None, primary_key=None):
    """
    Merges a graph into the database.

    Args:
        graph (Subgraph): The graph to merge.
        session (Session): The `session <https://neo4j.com/docs/api/python-driver/current/api.html#session>`_ to use.
        primary_label (str): The primary label to merge on. Has to be provided if the nodes themselves don't have a primary label (Default: None)
        primary_key (str): The primary key to merge on. Has to be provided if the graph elements themselves don't have a primary label (Default: None)
    """
    session.execute_write(graph.__db_merge__, primary_label=primary_label, primary_key=primary_key)


def push(graph: Subgraph, session: Session):
    """
    Updates local graph elements with the database. The graph needs to be already in the database.

    Args:
        graph (Subgraph): The graph to create.
        session (Session): The `session <https://neo4j.com/docs/api/python-driver/current/api.html#session>`_ to use.
    """
    session.execute_write(graph.__db_push__)

def pull(graph: Subgraph, session: Session):
    """
    Pulls remote changes to the graph to the local copy. The graph needs to be already in the database.

    Args:
        graph (Subgraph): The graph to create.
        session (Session): The `session <https://neo4j.com/docs/api/python-driver/current/api.html#session>`_ to use.
    """
    session.execute_read(graph.__db_pull__)


def match_nodes(session: Session, *labels: List[str], **properties: dict):
    """
    Matches nodes in the database.

    Args:
        labels (List[str]): The labels to match.
        session (Session): The `session <https://neo4j.com/docs/api/python-driver/current/api.html#session>`_ to use.
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

    records = session.run(*clause).data()
    # Convert to Node
    out = []
    for record in records:
        node = Node.from_dict(record['LABELS(n)'], record['n'], identity=record['ID(n)'])
        out.append(node)
    return out


def match_relationships(session: Session, from_node: Node =None, to_node:Node =None, rel_type: str =None, **properties: dict):
    """
    Matches relationships in the database.

    Args:
        session (Session): The `session <https://neo4j.com/docs/api/python-driver/current/api.html#session>`_ to use.
        from_node (Node): The node to match the relationship from (Default: None)
        to_node (Node): The node to match the relationship to (Default: None)
        rel_type (str): The type of the relationship to match (Default: None)
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
    records = session.run(*clause).data()
    out = []
    for record in records:
        fn = Node.from_dict(record['LABELS(from_node)'], record['from_node'], identity=record['ID(from_node)']) if from_node is None else from_node
        tn = Node.from_dict(record['LABELS(to_node)'], record['to_node'], identity=record['ID(to_node)']) if to_node is None else to_node
        rel = Relationship.from_dict(fn, tn, record['TYPE(r)'], record['PROPERTIES(r)'], identity=record['ID(r)'])
        out.append(rel)
    return out