"""Tests for common.utils module."""

from common.utils import get_value


class TestGetValue:
    def test_dict_access(self) -> None:
        obj = {"name": "test"}
        assert get_value(obj, "name") == "test"

    def test_object_attribute_access(self) -> None:
        class Obj:
            name = "test"

        assert get_value(Obj(), "name") == "test"

    def test_dict_missing_key_returns_none(self) -> None:
        assert get_value({}, "missing") is None

    def test_object_missing_attr_returns_none(self) -> None:
        class Obj:
            pass

        assert get_value(Obj(), "missing") is None
