Information for Developers
==========================

If you intend to extend the library, please check out the `class diagram of the core of the library <https://github.com/sg-dev/rel2graph/blob/main/docs/assets/pdfs/class_diagram_core.pdf>`_.

Testing
~~~~~~~

The library uses `pytest <https://docs.pytest.org>`_ for testing. 
Please implement tests for your code. Use the following snipped to run the tests.

.. code-block:: console
    
    python -m pytest

Any new tests are to be put in the *tests/{unit,integration}* folder respectively.
