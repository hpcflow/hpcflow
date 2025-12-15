import io
import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.core.errors import CompactException
from hpcflow.sdk.core.warnings import CompactWarning


@pytest.fixture(scope="module")
def enabled_formatter():
    """Disable the app's global formatter, replace with a test instance,
    restore afterwards."""
    # save any existing active instance
    if (orig := hf.CompactProblemFormatter._active_instance) is not None:
        orig.disable()

    # create test-specific instance
    stream = io.StringIO()
    fmt = hf.CompactProblemFormatter(output_stream=stream)
    fmt.enable()

    # provide the formatter (and its stream) to tests
    yield fmt, stream

    # teardown:
    fmt.disable()

    # restore original instance:
    if orig is not None:
        orig.enable()


@pytest.fixture
def run_formatter(enabled_formatter):
    """Return a helper function to run exceptions or warnings through the formatter."""
    fmt, stream = enabled_formatter

    def _run_exception(exc: Exception) -> str:
        """Run exception through formatter and return captured output."""
        # reset capture
        stream.seek(0)
        stream.truncate(0)
        fmt._excepthook(type(exc), exc, exc.__traceback__)
        return stream.getvalue()

    def _run_warning(warn_obj) -> str:
        """Run warning through formatter and return captured output."""
        # reset capture
        stream.seek(0)
        stream.truncate(0)
        fmt._showwarning(
            message=warn_obj,
            category=warn_obj.__class__,
            filename="<test>",
            lineno=1,
        )
        return stream.getvalue()

    # return both helpers
    return _run_exception, _run_warning


def test_descriptive_exception_includes_solution(run_formatter):
    run_exc, _ = run_formatter
    output = run_exc(CompactException(hf, "A problem exists!", solution="Fix it!"))
    assert "Error: CompactException" in output
    assert "A problem exists!" in output
    assert "Solution:" in output
    assert "Fix it!" in output


def test_descriptive_warning(run_formatter):
    _, run_warn = run_formatter
    output = run_warn(CompactWarning("Watch out!"))
    assert "Watch out!" in output
    assert "Warning: CompactWarning" in output
