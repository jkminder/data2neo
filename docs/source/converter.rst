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

.. code-block:: python

    from rel2graph import Converter
    from rel2graph.utils import load_file

    converter = Converter(load_file(conversion_schema_file), iterator, graph)

The |Converter| can utilise **multithreading**. When initialising you can set the number of parallel workers. Each worker operates in its own thread. 
Be aware that the committing to the graph is often still serialized, since the semantics require this (e.g. nodes must be committed before any relation or when [merging nodes](#merging-nodes) all the nodes must be serially committed). So the primary use-case of using multiple workers is if your resources are utilising a network connection (e.g. remote database) or if you require a lot of [matching](#match) in the graph (matching is parallelised).

.. code-block:: python

    converter = Converter(conversion_schema, iterator, graph, num_workers = 20)

**Attention:** If you enable more than 1 workers, ensure that all your :doc:`wrappers <wrapper>` support multithreading (add locks if necessary).

Statefullness
~~~~~~~~~~~~~
 
The converter is **stateful**. If you call the converter in a *jupyter notebook* and an exception is raised during the converter's execution (e.g. due to a KeyboardInterrupt, a network problem, an error in the conversion schema file), you can fix the problem and call the converter again. The converter will continue its work where it left off (the resource responsible for the exception, will be reconverted). **It is guaranteed that no resource is lost.**

The state of the converter is reset when *initialising it* or when *you change the underlying iterator* with ``converter.iterator = new_iterator``. So if you don't want the stateful behaviour, you need to do one of these two things.

Example 1
---------

You use OData Resources, and the network connection suddenly drops out.
In the first cell, you initially have created the converter object and called it.

.. code-block:: python

    converter = Converter(conversion_schema, iterator, graph)
    converter()

Now a ``ConnectionException`` is raised due to network problems. You can now fix the problem and then recall the converter in a new cell:

.. code-block:: python

    converter()

The converter will just continue where it left off. 

Example 2
---------

You have a small error in your :doc:`conversion schema <conversion_schema>` for a specific entity (e.g. a typo in an attribute key), which is not immediately a problem since you first process a lot of other entities. Again in the first cell, you initially have created the converter object and called it.

.. code-block:: python

    converter = Converter(conversion_schema, iterator, graph)
    converter()

Now, e.g. ``KeyError`` is raised since the attribute name was written slightly wrong. Instead of rerunning the whole conversion (which might take hours), you can fix the schema file and reload the schema file and recall the converter:

.. code-block:: python

    converter.reload_schema(conversion_schema)
    converter()

The converter will just continue where it left off with the new :doc:`conversion schema <conversion_schema>`. 

Data types
~~~~~~~~~~~

Neo4j supports the following datatypes: **Number** (int or float), **String**, **Boolean**, **Point** as well as **temporal types** (Date, Time, LocalTime, DateTime, LocalDateTime, Duration) (`more here <https://neo4j.com/docs/cypher-manual/current/syntax/values/>`_). 
The py2graph library does currently not support **Points**. For all other types it will keep the type of the input. So if your resource provides ints/floats it will commit them as ints/floats to the graph. 
If you require a specific conversion you need to create your own custom wrappers. For **temporal values** the library uses the datetime/date objects of the 
python `datetime <https://docs.python.org/3/library/datetime.html>`_ library. If you want to commit a date(time) value to the graph make sure it is a date(time) object. 
All inputs that are not of type: `numbers.Number <https://docs.python.org/3/library/numbers.html>`_ (includes int & float), str, bool, `date <https://docs.python.org/3/library/datetime.html>`_, 
`datetime <https://docs.python.org/3/library/datetime.html>`_ are converted to strings before beeing commited to neo4j.

For converting strings to datetime/date the library provides some predefined wrappers. See :doc:`here <common_modules>` for more details.

Logging and progress monitoring
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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


When calling a |Converter| instance you can provide a **progress bar** class, that will be used to display progress. 
The progress bar class must be an instance of the `tqdm <https://tqdm.github.io>`_ progress bar (or behave like it). 

.. code-block:: python

    from tqdm import tqdm
    converter(progress_bar = tqdm)

You can use any of the tdqm progress bars. For example to monitor the progress via telegram you can use:

.. code-block:: python

    from tqdm.contrib.telegram import tqdm
    pb = lambda **kwargs: tqdm(token="yourtoken", chat_id="yourchatid", **kwargs) # Config your tokens
    converter(progress_bar=pb)

.. |Resource| replace:: :py:class:`Resource <rel2graph.Resource>`
.. |Converter| replace:: :py:class:`Converter <rel2graph.Converter>`
.. |ResourceIterator| replace:: :py:class:`ResourceIterator <rel2graph.ResourceIterator>`
.. _neo4j: https://neo4j.com/
.. _py2neo: https://py2neo.org/2021.1/index.html
