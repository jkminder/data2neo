Customising Resource and ResourceIterator
=========================================

The |Resource| and |ResourceIterator| classes are abstract classes and need to be implemented for your specific relational input data type. 
The library comes with implementations for a few :ref:`existing relational modules <resource:existing relational modules>`. 
If you data type is not supported, you can easily create implementations for your data; 
see below for an explanation or check out `one of the already implemented modules <https://github.com/sg-dev/rel2graph/tree/main/rel2graph/relational_modules>`_. 


If you think your implementations could be helpful for others as well, read through the chapter :doc:`Information for Developers <information_for_developers>` and create a pull request.

Resource
~~~~~~~~

The |Resource| is simply a wrapper around a relational entity. To create your own resource, you create a new class and inherit from |Resource|. 
The parent constructor must be called with no arguments. Additionally, you need to implement following methods: ``type``, ``__getitem__``, ``__setitem__``, ``__repr__``. See the docs strings below for more details.

.. code-block:: python 

    from rel2graph import Resource

    class MyResource(Resource):
        def __init__(...your constructor parameters...):
            super().__init__()
            # your constructor code
            ...

        @property
        def type(self) -> str:
            """
            Returns the type of the resource as a string. The @property decorator makes this a property of the resource. The type is then accessible via resource.type without braces.
            """
            ...

        def __getitem__(self, key: str) -> str:
            """
            Returns the value at key 'key' of the underlying relational entity. 
            Makes an item retrievable via resource["mykey"].
            """
            ...
        
        def __setitem(self, key: str, value: str) -> None:
            """
            Sets/Changes the value of the underlying relational entity at key 'key'. 
            It must be able to change existing key and also add new keys.
            Makes an item settable via resource["mykey"] = "somevalue".
            """
            ...

        def __repr__(self) -> str:
            """
            Gets a string representation of the resource. Only used for logging.

            Should follow the format:
            NameOfResource 'TypeOfResource' (DetailsAboutResource)
            
            super().__repr__() returns "NameOfResource 'TypeOfResource'"

            Example-Implementation:
            f"{super().__repr__()} ({self.somedetail})"
            """
            return f"{super().__repr__()} ({... some detail about your resource ...})"
        

ResourceIterator
~~~~~~~~~~~~~~~~

If you create your own |Resource|, you also need to create a |ResourceIterator| for this |Resource| that allows the |Converter| to iterate over a set of resources. 
The iterator needs to be written such that it can be traversed multiple times (The |Converter| will traverse it once for all nodes and then again for all relations). 

Your iterator class must inherit from |ResourceIterator|, and its constructor must call the parent constructor with no arguments. 
Additionally, you need to implement following methods: ``next``, ``reset_to_first``, ``__len__``. See the docs strings below for more details. 
Note that your iterator can also traverse resources of different types.

.. code-block:: python

    from rel2graph import ResourceIterator

    class MyIterator(ResourceIterator):
        def __init__(self, ...your constructor parameters...) -> None:
            super().__init__()
            ...

        def next(self) -> Resource:
            """Gets the next resource that will be converted. Returns None if the range is traversed."""
            ...
        
        def reset_to_first(self) -> None:
            """Resets the iterator to point to the first element"""
            ...

        def __len__(self) -> None:
            """
            Returns the total amount of resources in the iterator. This is only required if you use a progress bar and is used to compute the percentage of completed work
            """
            ...

A note on **multithreading**: If you intend to multithread your conversion with multiple workers 
(see chapter :doc:`converter`), be aware that ``iterator.next()`` is not parallelised. 
If you want to leverage multiple threads for loading remote data, you must implement this in the |Resource| class (in ``__getitem__``).

IteratorIterator
~~~~~~~~~~~~~~~~

The |IteratorIterator| allows you to iterate over multiple "sub"-iterators. 
There are no restrictions on the "sub"-iterators, as long as they are of type |ResourceIterator|. Since an |IteratorIterator| is also of type |ResourceIterator|, it can be used recursively.

.. code-block:: python

    from rel2graph import IteratorIterator
    iterator1 = ... # An iterator
    iterator2 = ... # Another iterator
    itit = IteratorIterator([iterator1, iterator2])


Existing relational modules
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Pandas
------

With the :py:class:`PandasDataframeIterator <rel2graph.relational_modules.pandas.PandasDataframeIterator>` you can wrap a `pandas dataframe <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html?highlight=dataframe#pandas>`_. 
If you pass a pandas dataframe to the :py:class:`PandasDataframeIterator <rel2graph.relational_modules.pandas.PandasDataframeIterator>` it will automatically create :py:class:`PandasSeriesResource <rel2graph.relational_modules.pandas.PandasSeriesResource>` out of all rows (series) and iterate over them. 
Since a dataframe has no type associated, you need to also provide a type name.

.. code-block:: python

    from rel2graph.relational_modules.pandas import PandasDataframeIterator
    iterator = PandasDataframeIterator(pandas.Dataframe(...), "MyType")


OData
-----

With the :py:class:`ODataListIterator <rel2graph.relational_modules.odata.ODataListIterator>` you can iterate over a list of OData entities from `pyodata <https://pyodata.readthedocs.io/en/latest/>`_. 
The entities need to be of type or behave like `pyodata.v2.service.EntityProxy <https://pyodata.readthedocs.io/en/latest/>`_.

.. code-block:: python

    from rel2graph.relational_modules.odata import ODataListIterator
    iterator = ODataListIterator([entity1, entity2,...])



.. |Resource| replace:: :py:class:`Resource <rel2graph.Resource>`
.. |Converter| replace:: :py:class:`Converter <rel2graph.Converter>`
.. |ResourceIterator| replace:: :py:class:`ResourceIterator <rel2graph.ResourceIterator>`
.. |IteratorIterator| replace:: :py:class:`IteratorIterator <rel2graph.IteratorIterator>`

.. _neo4j: https://neo4j.com/
.. _py2neo: https://py2neo.org/2021.1/index.html
