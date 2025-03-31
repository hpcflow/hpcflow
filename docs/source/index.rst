.. hpcflow documentation master file, created by
   sphinx-quickstart on Fri Mar 11 11:31:36 2022.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to hpcFlow's documentation!
===================================

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   Install <installation>
   User Guide <user/index>
   Reference <reference/index>
   Development <development/index>

hpcFlow is a workflow system, principally designed for running on
High Performance Computing clusters.
It's usually used (as `matFlow <matflow_>`_, a derived software package) for Materials Science.

Unlike many workflow engines, hpcFlow supports running without an orchestration process
on the head node of the cluster, and can handle iterative looping over a sequence of
commands until a condition is satisfied (often a convergence criterion).

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. _matflow: https://docs.matflow.io/
