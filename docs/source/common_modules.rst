Common modules
==============

The rel2graph library comes with some predefined wrappers. To use them you must import them from the ``rel2graph.common_modules`` module. The following wrappers are available:

types
--------
.. code-block:: python

    import rel2graph.common_modules.types


**INT**
~~~~~~~

Converts the attribute to an integer. If the attribute is not an integer, it will try to convert it to an integer. If this fails, it will raise a ``ValueError``.

**FLOAT**
~~~~~~~~~

Converts the attribute to a float. If the attribute is not a float, it will try to convert it to a float. If this fails, it will raise a ``ValueError``.

**STR**
~~~~~~~

Converts the attribute to a string. If the attribute is not a string, it will try to convert it to a string. If this fails, it will raise a ``ValueError``.

**BOOL**
~~~~~~~~

Converts the attribute to a boolean. If the attribute is not a boolean, it will try to convert it to a boolean. If this fails, it will raise a ``ValueError``.


datetime
--------
.. code-block:: python

    import rel2graph.common_modules.datetime


**DATETIME**
~~~~~~~~~~~~

The DATETIME attribute wrapper allows to convert strings into datetime objects. It uses the datetime strptime function to convert the strings to datetime based on some formatting. The default formating string is ``"%Y-%m-%dT%H:%M:%S"``. You can provide your own formating string as static argument in the conversion schema: ``- datetime = DATETIME(entity.datetime_as_str, "%Y/%m/%d %H:%M:%S")``. 
If the provided argument is a datetime instead of a string, it will just remove any timezone information. Check the `datetime documentation <https://docs.python.org/3/library/datetime.html>`_ for details about how strptime works.

**DATE**
~~~~~~~~~~~~

The DATE attribute wrapper allows to convert strings into date objects. It uses the datetime strptime function to convert the strings to datetime based on some formatting and from there into just a date. 
The default formating string is ``"%Y-%m-%d"``. You can provide your own formating string as static argument in the conversion schema: ``- date = DATETIME(entity.date_as_str, "%Y/%m/%d  %H:%M:%S")``. 
Check the `datetime documentation <https://docs.python.org/3/library/datetime.html>`_ for details about how strptime works. If the attribute passed to DATE contains also time information, this is simply stripped away (the format string still must fit the exact format of your attribute). 
If the provided argument is a datetime instead of a string, it will just remove any timezone information and convert it to date.

**Note**: If you encounter the exception ``TypeError: Neo4j does not support JSON parameters of type datetime`` for the DATE/DATETIME wrappers, make sure that you use the bolt/neo4j scheme with your graph. Dates won't work over *http*: 

.. code-block:: python

    g = Graph(scheme="bolt", host="localhost", port=7687,  auth=('neo4j', 'password'))
