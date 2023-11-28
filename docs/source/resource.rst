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
The iterator is based on python iterators and needs to be written such that it can be traversed multiple times (The |Converter| will traverse it once for all nodes and then again for all relations). 

Your iterator class must inherit from |ResourceIterator|, and its constructor must call the parent constructor with no arguments. 
Additionally, you need to implement following methods: ``__iter__``, ``__len__`` and optionally ``__next__``. See the docs strings below for more details. 
Note that your iterator can also traverse resources of different types.

.. code-block:: python

    from rel2graph import ResourceIterator

    class MyIterator(ResourceIterator):
        def __init__(self, ...your constructor parameters...) -> None:
            super().__init__()
            ...

        def __next__(self) -> Resource:
            """
            Returns the next resource in the iterator based on the current state.
            """
            ...
        
        def __iter__(self) -> ResourceIterator:
            """
            Resets the iterator state and returns the iterator itself. You can also the __iter__ function to directly return an iterator and not 
            use __next__.
            """
            ...

        def __len__(self) -> None:
            """
            Returns the total amount of resources in the iterator. This is only required if you use a progress bar and is used to compute the percentage of completed work
            """
            ...

A note on **multiprocessing**: If you intend to parallelise your conversion with multiple workers 
(see chapter :doc:`converter`), be aware that ``next(iterator)`` is not parallelised. 
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

With the :py:class:`PandasDataFrameIterator <rel2graph.relational_modules.pandas.PandasDataFrameIterator>` you can wrap a `pandas dataframe <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html?highlight=dataframe#pandas>`_. 
If you pass a pandas dataframe to the :py:class:`PandasDataFrameIterator <rel2graph.relational_modules.pandas.PandasDataFrameIterator>` it will automatically create :py:class:`PandasSeriesResource <rel2graph.relational_modules.pandas.PandasSeriesResource>` out of all rows (series) and iterate over them. 
Since a dataframe has no type associated, you need to also provide a type name.

.. code-block:: python

    from rel2graph.relational_modules.pandas import PandasDataFrameIterator
    iterator = PandasDataFrameIterator(pandas.DataFrame(...), "MyType")


SQLite
------

With the :py:class:`SQLiteIterator <rel2graph.relational_modules.sqlite.SQLiteIterator>` you can iterate over a sqlite database. You need to provide a connection to the database.
You can also provide a list of tables to iterate over. If you do not provide a list of tables, the iterator will iterate over all tables in the database. Rel2graph requires primary keys, so if your tables do not have primary keys, you need to provide a dictionary with table, primary key pairs.

By default the Iterator will mix all tables together. If you want to iterate over tables one after another, you can set the ``mix_tables`` parameter to ``False``. 

The python implementation of sqlite will often throw warnings if a new process is spawned. You can disable these warnings by setting the ``check_same_thread`` parameter to ``False``. Rel2graph does not share the connection between processes, only the master processes requests data from the database.

.. code-block:: python

    from rel2graph.relational_modules.sqlite import SQLiteIterator
    import sqlite3

    connection = sqlite3.connect("mydatabase.db", check_same_thread=False)
    iterator = SQLiteIterator(connection, filter=["table1", "table2"], primary_keys={"table3": "id"})


.. |Resource| replace:: :py:class:`Resource <rel2graph.Resource>`
.. |Converter| replace:: :py:class:`Converter <rel2graph.Converter>`
.. |ResourceIterator| replace:: :py:class:`ResourceIterator <rel2graph.ResourceIterator>`
.. |IteratorIterator| replace:: :py:class:`IteratorIterator <rel2graph.IteratorIterator>`

.. _neo4j: https://neo4j.com/
.. _py2neo: https://py2neo.org/2021.1/index.html
