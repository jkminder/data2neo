Quick Start
===========

A quick example for converting data in a `pandas <https://pandas.pydata.org>`_ dataframe into a graph. The full example code can be found `here <https://github.com/sg-dev/rel2graph/tree/main/examples>`_. For more details, please checkout the full documentation. 
We first define a :doc:`convertion schema <conversion_schema>` in a YAML style config file. In this config file we specify, which entites are converted into which nodes and which relations. 

**schema.yaml**

.. code-block:: yaml

    ENTITY("Flower"):
        NODE("Flower") flower:
            - sepal_length = Flower.sepal_length
            - sepal_width = Flower.sepal_width
            - petal_length = Flower.petal_width
            - petal_width = append(Flower.petal_width, " milimeters")
        NODE("Species", "BioEntity") species:
            + Name = Flower.species
        RELATION(flower, "is", species):
        
    ENTITY("Person"):
        NODE("Person") person:
            + ID = Person.ID
            - FirstName = Person.FirstName
            - LastName = Person.LastName
        RELATION(person, "likes", MATCH("Species", Name=Person.FavoriteFlower)):
            - Since = "4ever"

The library itself has 2 basic elements, that are required for the conversion: the |Converter| that handles the conversion itself and an |ResourceIterator| that iterates over the relational data. The iterator can be implemented for arbitrary data in relational format. Rel2graph currently has preimplemented iterators under:

- ``rel2graph.relational_modules.odata``  for `OData <https://www.odata.org>`_ databases (based on `pyodata <https://pyodata.readthedocs.io>`_)
- ``rel2graph.relational_modules.pandas`` for `Pandas <https://pandas.pydata.org>`_ dataframes

We will use the :py:class:`PandasDataframeIterator <rel2graph.relational_modules.pandas.PandasDataframeIterator>` from ``rel2graph.relational_modules.pandas``. Further we will use the :py:class:`IteratorIterator <rel2graph.IteratorIterator>` that can wrap multiple iterators to handle multiple dataframes. 
Since a pandas dataframe has no type/table name associated, we need to specify the name when creating a :py:class:`PandasDataframeIterator <rel2graph.relational_modules.pandas.PandasDataframeIterator>`. We also define define a custom function ``append`` that can be refered to in the schema file and that appends a string to the attribute value. 
For an entity with ``Flower["petal_width"] = 5``, the outputed node will have the attribute ``petal_width = "5 milimeters"``.

.. code-block:: python

    from py2neo import Graph
    import pandas as pd 
    from rel2graph.relational_modules.pandas import PandasDataframeIterator 
    from rel2graph import IteratorIterator, Converter, Attribute, register_attribute_postprocessor
    from rel2graph.utils import load_file

    # Create a connection to the neo4j graph with the py2neo Graph object
    graph = Graph(scheme="http", host="localhost", port=7474,  auth=('neo4j', 'password')) 

    people = ... # a dataframe with peoples data (ID, FirstName, LastName, FavoriteFlower)
    people_iterator = PandasDataframeIterator(people, "Person")
    iris = ... # a dataframe with the iris dataset
    iris_iterator = PandasDataframeIterator(iris, "Flower")

    # register a custom data processing function
    @register_attribute_postprocessor
    def append(attribute, append_string):
        new_attribute = Attribute(attribute.key, attribute.value + append_string)
        return new_attribute

    # Create IteratorIterator
    iterator = IteratorIterator([pandas_iterator, iris_iterator])

    # Create converter instance with schema, the final iterator and the graph
    converter = Converter(load_file("schema.yaml"), iterator, graph)
    # Start the conversion
    converter()


.. |Resource| replace:: :py:class:`Resource <rel2graph.Resource>`
.. |Converter| replace:: :py:class:`Converter <rel2graph.Converter>`
.. |ResourceIterator| replace:: :py:class:`ResourceIterator <rel2graph.ResourceIterator>`