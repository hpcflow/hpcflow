Tips and tricks
===============

How to profile import times
---------------------------

Install with development dependencies and then run:

.. code-block:: bash

   python -X importtime -m hpcflow.cli --run-time-info 2> import.log
   tuna import.log

How to customise testing in the GitHub actions workflows
--------------------------------------------------------

Pytest command-line arguments can be passed to the test GHA workflow when manually triggered. For example, the following options will run only tests that match the name ``test_get_demo_data_cache``, will output all standard output/error streams (``s``), and will output logging at the specified level (``--log-cli-level``):

.. code-block:: bash

   -s -k test_get_demo_data_cache --log-cli-level=INFO
