from neo4j import Session

from .graph_elements import Node, Relationship, Subgraph, Attribute

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