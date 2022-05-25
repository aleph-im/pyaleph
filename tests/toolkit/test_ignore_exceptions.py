import pytest

from aleph.toolkit.exceptions import ignore_exceptions


def test_ignore_one_type_of_exception():
    some_dict = {"a": 1}

    with ignore_exceptions(KeyError):
        _ = some_dict["b"]

    assert some_dict["a"] == 1


def test_ignore_multiple_types_of_exceptions():
    some_dict = {"a": 1}

    with ignore_exceptions(KeyError, AttributeError):
        _ = some_dict["b"]

    with ignore_exceptions(KeyError, AttributeError):
        _ = some_dict.new_dict_method_from_python5()

    assert some_dict["a"] == 1


def test_ignore_no_exception():
    some_dict = {"a": 1}

    with pytest.raises(KeyError):
        with ignore_exceptions():
            _ = some_dict["b"]


def test_ignore_the_wrong_type_of_exception():
    some_dict = []

    with pytest.raises(AttributeError):
        with ignore_exceptions(IndexError):
            _ = some_dict.items()


def test_ignore_exception_with_callback():
    some_dict = {"a": 1}

    callback_was_called = False

    def callback(e):
        assert isinstance(e, KeyError)
        nonlocal callback_was_called
        callback_was_called = True

    with ignore_exceptions(KeyError, on_error=callback):
        _ = some_dict["b"]

    assert callback_was_called
