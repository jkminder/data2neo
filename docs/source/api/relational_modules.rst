------------------
Relational modules
------------------
The provided implementations for relational entities. 
They implement the abstract classes of the main interface.

=====
OData
=====

Implements the :py:class:`Resource <rel2graph.Resource>` and :py:class:`ResourceIterator <rel2graph.ResourceIterator>` for OData based on `pyodata <https://pyodata.readthedocs.io>`_.


ODataResource
~~~~~~~~~~~~~

.. autoclass:: rel2graph.relational_modules.odata.ODataResource
   :members:
   :show-inheritance:


ODataListItereator
~~~~~~~~~~~~~~~~~~

.. autoclass:: rel2graph.relational_modules.odata.ODataListIterator
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