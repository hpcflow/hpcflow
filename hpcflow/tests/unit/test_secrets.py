import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.core.errors import SecretNotFoundError


def test_set_get_delete_secret():
    key, value = "SECRET_KEY", "SECRET_VALUE"
    hf.set_secret(key, value)
    assert hf.get_secret(key) == value
    hf.delete_secret(key)
    with pytest.raises(SecretNotFoundError):
        hf.get_secret(key)
