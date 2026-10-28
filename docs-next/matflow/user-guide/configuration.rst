Local and cluster configuration
===============================

Keep machine-specific settings out of reusable task schemas.
First run a small workflow locally, then check the same software directly on the cluster.

Slurm
-----

Describe available partitions, resource limits, default directives and any account
requirements in the ``schedulers.slurm`` configuration. Request only resources
that your selected executable supports. Check the generated jobscript before a
large submission; site policy can require account, partition or memory directives.

SGE
---

Use your site's queue and parallel-environment settings. Confirm that the requested
core count agrees with the application's launch command. A working Slurm configuration
cannot be copied unchanged to SGE.

Shell setup
-----------

Check that modules and activation commands work in the non-interactive shell used
by a jobscript. An executable that works in an interactive terminal may depend on
startup files that are not loaded by a batch job.

Use ``matflow open config`` to open the configuration and ``matflow open env-source``
to inspect environment sources. Existing detailed configuration examples remain
available through :doc:`../reference/index`.
