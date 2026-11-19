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

Testing optional pytest hooks in bundled executables
---------------------------------------------------

PyInstaller executables run the bundled test suite without loading
``pytest-xdist``. Hooks provided by that plugin, such as
``pytest_configure_node``, must use ``@pytest.hookimpl(optionalhook=True)`` so
pytest can also collect tests when the plugin is absent.

To check this configuration from a source checkout, set the environment variable
``PYTEST_DISABLE_PLUGIN_AUTOLOAD=1`` and run:

.. code-block:: bash

   python -m pytest -p hpcflow.pytest_plugin --configure-python-env hpcflow/tests/unit/test_pytest_plugin.py hpcflow/tests/unit/test_cli.py -q

Unset the variable afterwards. Also run the hook regression tests with xdist
enabled to check worker configuration propagation:

.. code-block:: bash

   python -m pytest -p hpcflow.pytest_plugin --configure-python-env hpcflow/tests/unit/test_pytest_plugin.py -q -n 2
