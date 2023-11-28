-----------------
Neo4j Integration
-----------------

These functions abstract complexity of interacting with Neo4j. Instead of writing Cypher queries, you can use Python objects to create and merge nodes and relationships.

.. autofunction:: rel2graph.neo4j.create

.. autofunction:: rel2graph.neo4j.merge

Subgraph
~~~~~~~~

.. autoclass:: rel2graph.neo4j.Subgraph
   :members:

Node
~~~~

.. autoclass:: rel2graph.neo4j.Node
   :members:
   :show-inheritance:


Relationship
~~~~~~~~~~~~

.. autoclass:: rel2graph.neo4j.Relationship
   :members:
   :show-inheritance: