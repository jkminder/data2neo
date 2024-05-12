-----------------
Neo4j Integration
-----------------

These functions abstract complexity of interacting with Neo4j. Instead of writing Cypher queries, you can use Python objects to create, merge and match nodes and relationships.

.. autofunction:: data2neo.neo4j.create

.. autofunction:: data2neo.neo4j.merge

.. autofunction:: data2neo.neo4j.push

.. autofunction:: data2neo.neo4j.pull

.. autofunction:: data2neo.neo4j.match_nodes

.. autofunction:: data2neo.neo4j.match_relationships

Subgraph
~~~~~~~~

.. autoclass:: data2neo.neo4j.Subgraph
   :members:

Node
~~~~

.. autoclass:: data2neo.neo4j.Node
   :members:
   :show-inheritance:


Relationship
~~~~~~~~~~~~

.. autoclass:: data2neo.neo4j.Relationship
   :members:
   :show-inheritance: