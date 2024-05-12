------------------
Relational modules
------------------
The provided implementations for relational entities. 
They implement the abstract classes of the main interface.

======
SQLite
======

Implements the :py:class:`Resource <data2neo.Resource>` and :py:class:`ResourceIterator <data2neo.ResourceIterator>` for sqlite based on `sqlite3 <https://docs.python.org/3/library/sqlite3.html>`_.


SQLiteResource
~~~~~~~~~~~~~~

.. autoclass:: data2neo.relational_modules.sqlite.SQLiteResource
   :members:
   :show-inheritance:


SQLiteIterator
~~~~~~~~~~~~~~~~~~

.. autoclass:: data2neo.relational_modules.sqlite.SQLiteIterator
   :members:
   :show-inheritance:

======
Pandas
======

Implements the :py:class:`Resource <data2neo.Resource>` and :py:class:`ResourceIterator <data2neo.ResourceIterator>` for `pandas <https://pandas.pydata.org/>`_.

PandasSeriesResource
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: data2neo.relational_modules.pandas.PandasSeriesResource
   :members:
   :show-inheritance:


PandasDataFrameIterator
~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: data2neo.relational_modules.pandas.PandasDataFrameIterator
   :members:
   :show-inheritance: