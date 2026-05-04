import enum


class SkipReason(enum.Enum):
    NOT_SKIPPED = 0
    UPSTREAM_FAILURE = 1
    LOOP_TERMINATION = 2
