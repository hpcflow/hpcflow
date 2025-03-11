"""Core programmatic models for hpcflow.

EAR abort exit code is set to 64. EAR skipped exit code is set to 65.

References
----------
https://tldp.org/LDP/abs/html/exitcodes.html

"""

import numpy as np

#: Formats supported for templates.
ALL_TEMPLATE_FORMATS = ("yaml", "json")
#: The exit code used by an EAR when it aborts.
ABORT_EXIT_CODE = 64
SKIPPED_EXIT_CODE = 65
NO_COMMANDS_EXIT_CODE = 66
RUN_DIR_ARR_DTYPE = [
    ("task_insert_ID", np.uint8),
    ("element_idx", np.uint32),
    ("iteration_idx", np.uint32),
    ("action_idx", np.uint8),
    ("run_idx", np.uint8),
    ("element_depth", np.uint8),
    ("iteration_depth", np.uint8),
]
_uint8_max = np.iinfo(np.uint8).max
_uint32_max = np.iinfo(np.uint32).max
RUN_DIR_ARR_FILL = (
    _uint8_max,
    _uint32_max,
    _uint32_max,
    _uint8_max,
    _uint8_max,
    _uint8_max,
    _uint8_max,
)
