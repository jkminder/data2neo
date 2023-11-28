Update from pre 1.0.0 
-----------------------------

If you have been using rel2graph prior to version 1.0.0, you will need to update your code. The following changes have been made:

RELATION to RELATIONSHIP
~~~~~~~~~~~~~~~~~~~~~~~~
The keyword `RELATION` has been renamed to `RELATIONSHIP`. This is to be consistent with the naming scheme in neo4j.
Simply replace all instances of `RELATION` with `RELATIONSHIP` in your schema.


Py2neo to native neo4j driver
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

With the release of rel2graph 1.0.0, the underlying driver for neo4j has been changed from py2neo to the native neo4j driver. Py2neo has been deprecated and is no longer supported. 
This means that you will need to update your code to use the new driver. The following changes have been made:

1. The |Converter| now takes a uri and credentials instead of a graph object.

.. code-block:: python

    # Old
    from py2neo import Graph
    from rel2graph import Converter

    graph = Graph("bolt://localhost:7687", auth=("neo4j", "password"))
    converter = Converter(schema, iterator, graph)

    # New
    from rel2graph import Converter
    import neo4j

    uri = "bolt://localhost:7687"
    auth = neo4j.basic_auth("neo4j", "password")

    converter = Converter(schema, iterator, uri, auth)

2. If you use wrappers that operate on the py2neo graph object, you will need to update them to use the native neo4j driver. The :ref:`Global Shared State <converter:Global Shared State>` provides the neo4j driver with ``GlobalSharedState.graph_driver``. Further the :doc:`neo4j <../neo4j>` module provides the |Node|, |Relationship| and |Subgraph| classes, similar to py2neo. To push a subgraph to the neo4j database, use the :py:func:`create <rel2graph.neo4j.create>` and :py:func:`merge <rel2graph.neo4j.create>`  methods.

.. code-block:: python

    # Old
    from py2neo import Graph
    from rel2graph import register_subgraph_postprocessor
    from py2neo import Node

    @register_subgraph_postprocessor
    def add_node(subgraph):
        # create a new node
        node = Node("Label", name="name")
        subgraph |= node
        return subgraph

    @register_subgraph_postprocessor
    def change_graph(graph, subgraph):
        # add a new node to the graph
        GlobalSharedState.graph.create(Node("Label", name="name"))
        return subgraph

    # New
    from rel2graph import register_subgraph_postprocessor, GlobalSharedState
    from rel2graph.neo4j import create, Node, Subgraph, Relationship

    @register_subgraph_postprocessor
    def add_node(subgraph):
        # create a new node
        node = Node("Label", name="name")
        subgraph |= node
        return subgraph

    @register_subgraph_postprocessor
    def change_graph(graph, subgraph):
        # add a new node to the graph
        with GlobalSharedState.graph_driver.session() as session:
            create(Node("Label", name="name"), session) # or merge
        return subgraph




.. |Resource| replace:: :py:class:`Resource <rel2graph.Resource>`
.. |Converter| replace:: :py:class:`Converter <rel2graph.Converter>`
.. |ResourceIterator| replace:: :py:class:`ResourceIterator <rel2graph.ResourceIterator>`
.. |Attribute| replace:: :py:class:`Attribute <rel2graph.Attribute>`
.. |SubgraphWrapper| replace:: :py:class:`SubgraphFactoryWrapper <rel2graph.SubgraphFactoryWrapper>`
.. |AttributeWrapper| replace:: :py:class:`AttributeFactoryWrapper <rel2graph.AttributeFactoryWrapper>`
.. |Subgraph| replace:: :py:class:`Subgraph <rel2graph.neo4j.Subgraph>`
.. |Node| replace:: :py:class:`Node <rel2graph.neo4j.Node>`
.. |Relationship| replace:: :py:class:`Relationship <rel2graph.neo4j.Relationship>`