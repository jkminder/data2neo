Neo4j Integration
=================

The rel2graph library comes with a set of abstract classes that simplify the interaction with neo4j in python. They are derived from the now EOL library py2neo.
This includes python objects to represent |Node| and |Relationship| objects as well as a |Subgraph| object that can be used to represent a set of nodes and relationships.
|Node| and |Relationship| objects are themself a |Subgraph|. The functions :py:func:`create <rel2graph.neo4j.create>` and :py:func:`merge <rel2graph.neo4j.merge>` can be used to create or merge a |Subgraph| into a neo4j database given a neo4j session. To sync local
|Subgraph| objects with the database, use the :py:func:`push <rel2graph.neo4j.push>` and :py:func:`pull <rel2graph.neo4j.pull>` functions.Further use the functions :py:func:`match_nodes <rel2graph.neo4j.match_nodes>` and :py:func:`match_relationships <rel2graph.neo4j.match_relationships>` to match elements in the graph and return a list of |Node| or |Relationship|.
We refer to the :doc:`neo4j documentation <api/neo4j>` for more information.

.. |Subgraph| replace:: :py:class:`Subgraph <rel2graph.neo4j.Subgraph>`
.. |Node| replace:: :py:class:`Node <rel2graph.neo4j.Node>`
.. |Relationship| replace:: :py:class:`Relationship <rel2graph.neo4j.Relationship>`
