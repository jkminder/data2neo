Neo4j Integration
=================

The Data2Neo library comes with a set of abstract classes that simplify the interaction with neo4j in python. They are derived from the now EOL library py2neo.
This includes python objects to represent |Node| and |Relationship| objects as well as a |Subgraph| object that can be used to represent a set of nodes and relationships.
|Node| and |Relationship| objects are themself a |Subgraph|. The functions :py:func:`create <data2neo.neo4j.create>` and :py:func:`merge <data2neo.neo4j.merge>` can be used to create or merge a |Subgraph| into a neo4j database given a neo4j session. To sync local
|Subgraph| objects with the database, use the :py:func:`push <data2neo.neo4j.push>` and :py:func:`pull <data2neo.neo4j.pull>` functions.Further use the functions :py:func:`match_nodes <data2neo.neo4j.match_nodes>` and :py:func:`match_relationships <data2neo.neo4j.match_relationships>` to match elements in the graph and return a list of |Node| or |Relationship|.
We refer to the :doc:`neo4j documentation <api/neo4j>` for more information.

.. |Subgraph| replace:: :py:class:`Subgraph <data2neo.neo4j.Subgraph>`
.. |Node| replace:: :py:class:`Node <data2neo.neo4j.Node>`
.. |Relationship| replace:: :py:class:`Relationship <data2neo.neo4j.Relationship>`
