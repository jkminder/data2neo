------------------
Relational modules
------------------
The provided implementations for relational entities. 
They implement the abstract classes of the main interface.

======
SQLite
======

Implements the :py:class:`Resource <rel2graph.Resource>` and :py:class:`ResourceIterator <rel2graph.ResourceIterator>` for sqlite based on `sqlite3 <https://docs.python.org/3/library/sqlite3.html>`_.


SQLiteResource
~~~~~~~~~~~~~~

.. autoclass:: rel2graph.relational_modules.sqlite.SQLiteResource
   :members:
   :show-inheritance:


SQLiteIterator
~~~~~~~~~~~~~~~~~~

.. autoclass:: rel2graph.relational_modules.sqlite.SQLiteIterator
   :members:
   :show-inheritance:

======
Pandas
======

Implements the :py:class:`Resource <rel2graph.Resource>` and :py:class:`ResourceIterator <rel2graph.ResourceIterator>` for `pandas <https://pandas.pydata.org/>`_.

PandasSeriesResource
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: rel2graph.relational_modules.pandas.PandasSeriesResource
   :members:
   :show-inheritance:


PandasDataframeIterator
~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: rel2graph.relational_modules.pandas.PandasDataframeIterator
   :members:
   :show-inheritance: