.. jinja:: first_ctx

  Common errors
  #############

  Certain errors have cropped up multiple times for {{ app_name }} users.
  Here's some advice for those errors.

  Submitting a workflow
  -----------------------------

  If you get an error which (often) starts with

  .. code-block:: console

      ERROR {{ app_module }}.persistence: batch update exception!

  and ends with something like

  .. code-block:: console

      File "hpcflow/sdk/app.py", line 1150, in read_known_submissions_file
      File "hpcflow/sdk/app.py", line 1122, in _parse_known_submissions_line
      ValueError: not enough values to unpack (expected 8, got 6)

  This is usually caused by updating the {{ app_name }} version.
  Leftover submissions info causes the newer {{ app_name }} version to get confused.
  The fix? ``{{ app_package_name }} manage clear-known-subs``.
  This will delete the known submissions file, and the next time you submit a workflow,
  {{ app_name }} will create a new one.
