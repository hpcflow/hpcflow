:orphan:

.. _install:

.. jinja:: first_ctx

    ############
    Installation
    ############

    There are two ways of using {{ app_name }}:
    
    * The {{ app_name }} command-line interface (CLI)
    * The {{ app_name }} Python package

    Both of these options allow workflows to be designed and executed. The {{ app_name }} CLI
    is recommended for beginners and strongly recommended if you want to 
    run {{ app_name }} on a cluster. The Python package allows workflows to be
    designed and explored via the Python API and is recommended for users 
    comfortable working with Python. If you are interested in contributing to 
    the development of {{ app_name }}, the Python package is the place to start.

    The CLI and the Python package can be used simultaneously.

    Using pip
    ==========================

    The recommended way to install {{ app_name }} is to
    use pip to install the Python package from PyPI::

      pip install {{ dist_name }}

    This installs the python package, which also gives the CLI version of {{ app_name }}.

    *************
    Release notes
    *************

    Release notes for this version ({{app_version}}) are `available on GitHub <https://github.com/{{ github_user }}/{{ github_repo }}/releases/tag/v{{ app_version }}>`_.
    Use the version switcher in the top-right corner of the page to download/install other versions.

    Alternative installation methods
    ================================
    Although *not currently recommended*,
    advanced users may wish to use one of the :ref:`alternative installation methods <alternative_install>`.


    #############
    Configuration
    #############

    {{ app_name }} uses a config file to control details of how it executes workflows.
    A :ref:`default config file <default_config>` will be created the first time you submit a workflow.
    This will work without modification on a personal machine,
    however if you are using {{ app_name }} on HPC you will likely need to make some
    modifications to describe the job scheduler, and settings for multiple cores,
    and to point to your {{ app_name }} environments file.

    `Some examples <https://github.com/hpcflow/matflow-configs>`_ are given
    for the University of Manchester's CSF.

    The path to your config file can be found using ``{{ app_module }} manage get-config-path``,
    or to open the config file directly, use ``{{ app_module }} open config``.

    #############
    Environments
    #############

    {{ app_name }} has the concept of environments, similar to python virtual environments.
    These are required so that tasks can run using the specific software they require.
    Your {{ app_name }} environments must be defined in your environments (YAML) file before {{ app_name }}
    can run workflows, and this environment file must be pointed to in the config file
    via the ``environment_sources`` key.
    Once this has been done, your environment file can be be opened using ``{{ app_module }} open env-source``.

    Below is an example environments file that defines an environment for running Pyton scripts.
    Domain-specific tools can be added to the environments file as required, each with their own 
    setup instructions for loading that tool on your machine.

    .. code-block:: yaml

      - name: python_env
        executables:
          - label: python_script
            instances:
        - command: python "<<script_path>>" <<args>>
          num_cores:
            start: 1
            stop: 32
          parallel_mode: null
