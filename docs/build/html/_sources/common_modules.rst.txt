Common modules
==============

The rel2graph library comes with some predefined wrappers. To use any of them you must import them:

.. code-block:: python

    import rel2graph.common_modules


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
