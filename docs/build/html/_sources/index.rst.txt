.. rel2graph documentation master file, created by
   sphinx-quickstart on Wed Apr 20 19:25:41 2022.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to **rel2graph**'s documentation!
=========================================

**rel2graph** is a library that simplifies the conversion of data in relational format 
to a graph knowledge database. It relieves you of the cumbersome manual work of writing the conversion code 
and lets you focus on the conversion schema and data processing.

The library is built specifically for converting data into a neo4j_ graph. 
The library further supports extensive customisation capabilities to clean and remodel data. 
As neo4j python client it uses the py2neo_ library.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   Quick Start <quick_start>
   Introduction <introduction>
   Converter <converter>
   Schema syntax <conversion_schema>
   resource
   wrapper
   common_modules
   py2neo_extensions
   information_for_developers
   API reference <api/api>


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. |Resource| replace:: :py:class:`Resource <rel2graph.Resource>`
.. |Converter| replace:: :py:class:`Converter <rel2graph.Converter>`
.. |ResourceIterator| replace:: :py:class:`ResourceIterator <rel2graph.ResourceIterator>`
.. _neo4j: https://neo4j.com/
.. _py2neo: https://py2neo.org/2021.1/index.html
