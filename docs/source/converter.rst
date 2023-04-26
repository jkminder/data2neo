Converter
=========

The |Converter| handles the main conversion of the relational data. 
It is initialised with the *conversion schema* as a string, the iterator and the graph. 

.. code-block:: python

    from rel2graph import Converter

    converter = Converter(conversion_schema, iterator, graph)

To start the conversion, one simply calls the object. It then iterates twice over the iterator: first to process all the nodes and, secondly, to create all relations. This makes sure that any node a relation refers to is already created first.

.. code-block:: python

    converter()

If your conversion schema is saved in a seperate file you can use the provided ``load_file`` function to load it into a string.
When calling a |Converter| instance you can provide a **progress bar** class, that will be used to display progress. 
The progress bar class must be an instance of the `tqdm <https://tqdm.github.io>`_ progress bar (or follow its API). 

.. code-block:: python

    from rel2graph import Converter
    from rel2graph.utils import load_file
    from tqdm import tqdm

    converter = Converter(load_file(conversion_schema_file), iterator, graph)
    converter(progress_bar = tqdm)

By default, the |Converter| uses multiple processes to speed up the conversion
process by dividing the resources into batches and distributing
them among the available processes. The number of worker
processes and the batch size can be customized with the
parameters ``num_workers`` and ``batch_size`` (default values are
``number of cores - 2`` and ``10000``, respectively).  It's important
to note that the transfer of data to the graph is always serialized
to ensure correctness. If the Neo4j instance is running locally,
ensure that you have sufficient resources for the database as a
large portion of the processing power required for conversion
is used by the database.

.. code-block:: python

    converter = Converter(conversion_schema, iterator, graph, num_workers=10, batch_size=20000)

**Attention:** Ensure that all your :doc:`wrappers <wrapper>` are free of dependencies between resources. Because results are batched and parallelised, the order of the commits to the graph may be different than the order in which your iterator yields the resources. 
Further if your wrappers need to have a state you need to make sure that the state is shared between the processes. Refer to the :ref:`Global Shared State <converter:Global Shared State>` chapter for details.
If you require serialized processing of your resources or limitation to a single process you can use the ``serialize`` option. This will disable multiprocessing and process the resources one after the other in a single process.
Note that this will make the conversion significantly slower.

.. code-block:: python

    converter = Converter(conversion_schema, iterator, graph, serialize = True)


Data types
~~~~~~~~~~~

Neo4j supports the following datatypes: **Number** (int or float), **String**, **Boolean**, **Point** as well as **temporal types** (Date, Time, LocalTime, DateTime, LocalDateTime, Duration) (`more here <https://neo4j.com/docs/cypher-manual/current/syntax/values/>`_). 
The py2graph library does currently not support **Points**. For all other types it will keep the type of the input. So if your resource provides ints/floats it will commit them as ints/floats to the graph. 
If you require a specific conversion you need to create your own custom wrappers. For **temporal values** the library uses the datetime/date objects of the 
python `datetime <https://docs.python.org/3/library/datetime.html>`_ library. If you want to commit a date(time) value to the graph make sure it is a date(time) object. 
All inputs that are not of type: `numbers.Number <https://docs.python.org/3/library/numbers.html>`_ (includes int & float), str, bool, `date <https://docs.python.org/3/library/datetime.html>`_, 
`datetime <https://docs.python.org/3/library/datetime.html>`_ are converted to strings before beeing commited to neo4j.

For converting strings to datetime/date the library provides some predefined wrappers. See :doc:`here <common_modules>` for more details.

Global Shared State
~~~~~~~~~~~~~~~~~~~

If you need to share state between your wrappers you must notify rel2graph explicitly about this. An example of this is a wrapper that needs to keep track of the number of resources it has processed. Because the |Converter| uses multiple workers
the wrapper may be called in different processes and the state is not shared between the processes (note that parallel processes do not share the same memory). To share state between the processes you need to use the ``GlobalSharedState``. The ``GlobalSharedState`` is a singleton class that can be used to share state between the processes.
Before calling the |Converter| you need to register your state with the ``GlobalSharedState`` by simply defining an attribute on it ``GlobalSharedState.my_state = my_state``. By default the ``GlobalSharedState`` will provide the py2neo graph object to every process under ``GlobalSharedState.graph``. 

Note that the ``GlobalSharedState`` only makes sure that the variable you give it is passed to all processes. You need to make sure that the variable is sharable between the processes. For example if you want to share a counter between the processes you need to use a ``multiprocessing.Value``. 
Other options include ``multiprocessing.Array``, ``multiprocessing.Queue``, ``multiprocessing.Pipe`` and ``multiprocessing.Manager``. See the `multiprocessing documentation <https://docs.python.org/3/library/multiprocessing.html#sharing-state-between-processes>`_ for more details.

.. code-block:: python

    from rel2graph import GlobalSharedState, register_subgraph_preprocessor


    @register_subgraph_preprocessor
    def COUNT_RESOURCES(resource)
        GlobalSharedState.count += 1
        return resource

    """
    The following doesn't work because the state is not shared between the processes:

    count = 0
    @register_subgraph_preprocessor
    def COUNT_RESOURCES(resource)
        count += 1
        return resource
    """

    @register_subgraph_preprocessor
    def DO_SMTH_WITH_THE_GRAPH(resource)
        GlobalSharedState.graph.run("CREATE (n:Node {name: 'test'})")
        return resource

    # First register your state with the GlobalSharedState
    from multiprocessing import Value
    GlobalSharedState.count = Value('i', 0)

    # Now you can start the conversion, the state is shared between the processes
    converter = Converter(conversion_schema, iterator, graph)
    converter(progress_bar = tqdm)
    
        
Logging
~~~~~~~

The whole rel2graph library uses the standard python `logging <https://docs.python.org/3/howto/logging.html>`_ library. 
See an example of how to use it below. For more information, check out the `official documentation <https://docs.python.org/3/howto/logging.html>`_.

.. code-block:: python
        
    import logging

    logger = logging.getLogger("rel2graph") # Get Logger
    logger.setLevel(logging.DEBUG) # Set the log level to DEBUG
    log_formatter = logging.Formatter("%(asctime)s [%(threadName)s]::[%(levelname)s]::%(filename)s: %(message)s") # Specify the format
    console_handler = logging.StreamHandler() # Create console handler (will output directly to console)
    console_handler.setFormatter(log_formatter) # Add formater to handler
    logger.addHandler(console_handler) # add handler to logger

Peformance Optimization
~~~~~~~~~~~~~~~~~~~~~~~

MATCH clauses
^^^^^^^^^^^^^
While the MATCH clause is very flexible it comes with an overhead, that can be avoided if some assumptions can be made about the data.
If you know that your MATCH clause will always return a single node and you match by exactly one property it is faster to 
merge the node instead of matching it. This will allow the backend to process the merges in batches and will be much faster.

Replace this:

.. code-block::

    ENTITY("Name"):
        NODE("Label") source:
            ...    

        RELATION(source, "TO", MATCH("Target", uid=Name.uid)):

With this:

.. code-block:: 

    ENTITY("Name"):
        NODE("Source") source:
            ...    

        NODE("Target") target:
            + uid = Name.uid 
        
        RELATION(source, "TO", target):

.. |Resource| replace:: :py:class:`Resource <rel2graph.Resource>`
.. |Converter| replace:: :py:class:`Converter <rel2graph.Converter>`
.. |ResourceIterator| replace:: :py:class:`ResourceIterator <rel2graph.ResourceIterator>`
.. _neo4j: https://neo4j.com/
.. _py2neo: https://py2neo.org/2021.1/index.html
.. _tqdm: https://tqdm.github.io
