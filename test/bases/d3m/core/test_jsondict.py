from decimal import Decimal
from types import MappingProxyType
import datetime as dt
from uuid import UUID

import pytest

from d3m.core.types import JsonDict, FrozenJsonDict


class TestJsonDict:
    @pytest.mark.parametrize(
        "value",
        (
            True,
            False,
            None,
            123,
            456,
            123.45,
            456.78,
            "abc",
            "xyz",
        ),
    )
    def test_convert_simple_types(self, value):
        assert JsonDict({"value": value}) == {"value": value}
        assert JsonDict(value=value) == {"value": value}

    @pytest.mark.parametrize(
        "value, type_",
        (
            ((1, "2", True, None), tuple),
            ([1, "2", True, None], list),
            ({1, "2", True, None}, list),
            (frozenset((1, "2", True, None)), list),
        ),
    )
    def test_convert_iterable_items(self, value, type_):
        assert JsonDict({"value": value}) == {"value": type_(value)}
        assert JsonDict(value=value) == {"value": type_(value)}
        assert type(JsonDict(value=value)["value"]) is type_

    @pytest.mark.parametrize(
        "value",
        (
            {1: 1, 2: "2", 3: True, 4: [1, "a", True]},
            MappingProxyType({1: 1, 2: "2", 3: True, 4: [1, "a", True]}),
        ),
    )
    def test_convert_mapping_items(self, value):
        assert JsonDict({"value": value}) == {"value": value}
        assert JsonDict(value=value) == {"value": value}
        assert type(JsonDict(value=value)["value"]) is JsonDict

    @pytest.mark.parametrize(
        "value, result",
        (
            (dt.datetime(2023, 11, 23, 10, 11, 12, 789), "2023-11-23T10:11:12.000789"),
            (
                dt.datetime(2023, 11, 23, 10, 11, 12, 789, dt.timezone.utc),
                "2023-11-23T10:11:12.000789+00:00",
            ),
            (dt.date(2023, 11, 23), "2023-11-23"),
            (dt.date(2023, 11, 24), "2023-11-24"),
        ),
    )
    def test_date_value(self, value, result):
        assert JsonDict({"value": value}) == {"value": result}
        assert JsonDict(value=value) == {"value": result}

    def test_validate_when_set_item(self):
        obj = JsonDict()

        obj["dict"] = {}
        assert isinstance(obj["dict"], JsonDict)
        obj["dict"]["date"] = dt.date(2023, 11, 23)

        assert obj["dict"]["date"] == "2023-11-23"

    def test_recursive_dict(self):
        d = {"dict": {}}
        d["dict"]["recursive"] = d["dict"]
        with pytest.raises(ValueError, match="Circular reference detected"):
            JsonDict(d)

        d = {"list": []}
        d["list"].append(d["list"])
        with pytest.raises(ValueError, match="Circular reference detected"):
            JsonDict(d)

        d = JsonDict(dict={})
        with pytest.raises(ValueError, match="Circular reference detected"):
            d["dict"]["recursive"] = d

    def test_set_default(self):
        obj = JsonDict()
        obj.setdefault("a", {})

        assert isinstance(obj["a"], JsonDict)

    def test_update(self):
        obj = JsonDict(a=1)
        obj.update({"a": {}}, b=set())
        assert obj == {"a": {}, "b": []}
        assert isinstance(obj["a"], JsonDict)
        assert isinstance(obj["b"], list)

    def test_repr(self):
        obj = JsonDict(
            {
                "str": "abc",
                "int": 123,
                "bool": True,
                "null": None,
                "float": 123.45,
                "dict": {
                    "list": ["a", 1, None],
                    "set": {1, 2, 3},
                    "tuple": ("a", 1, None, dt.date(2023, 11, 24)),
                    "frozenset": {1, 2, 3},
                    "datatime": dt.datetime(
                        2023, 11, 23, 10, 11, 12, 789, dt.timezone.utc
                    ),
                },
                "uuid": UUID(int=1),
                "decimal": Decimal("0.01"),
            }
        )
        assert repr(obj) == (
            "{'str': 'abc', 'int': 123, 'bool': True, 'null': None, 'float': 123.45, "
            "'dict': {'list': ['a', 1, None], 'set': [1, 2, 3], 'tuple': ('a', 1, None, "
            "'2023-11-24'), 'frozenset': [1, 2, 3], 'datatime': "
            "'2023-11-23T10:11:12.000789+00:00'}, 'uuid': "
            "'00000000-0000-0000-0000-000000000001', 'decimal': '0.01'}"
        )

    def test_str(self):
        obj = JsonDict(
            {
                "str": "abc",
                "int": 123,
                "bool": True,
                "null": None,
                "float": 123.45,
                "dict": {
                    "list": ["a", 1, None],
                    "set": {1, 2, 3},
                    "tuple": ("a", 1, None, dt.date(2023, 11, 24)),
                    "frozenset": {1, 2, 3},
                    "datatime": dt.datetime(
                        2023, 11, 23, 10, 11, 12, 789, dt.timezone.utc
                    ),
                },
                "uuid": UUID(int=1),
                "decimal": Decimal("0.01"),
            }
        )
        assert str(obj) == (
            '{"str": "abc", "int": 123, "bool": true, "null": null, "float": 123.45, '
            '"dict": {'
            '"list": ["a", 1, null], "set": [1, 2, 3], '
            '"tuple": ["a", 1, null, "2023-11-24"], '
            '"frozenset": [1, 2, 3], '
            '"datatime": "2023-11-23T10:11:12.000789+00:00"}, '
            '"uuid": "00000000-0000-0000-0000-000000000001", '
            '"decimal": "0.01"}'
        )


class TestFreezeJsonDict:
    def test_type(self):
        assert isinstance(FrozenJsonDict({}), JsonDict)

    @pytest.mark.parametrize(
        "value",
        (
            (1, "2", True, None),
            [1, "2", True, None],
            {1, "2", True, None},
            frozenset((1, "2", True, None)),
        ),
    )
    def test_convert_iterable_items(self, value):
        assert FrozenJsonDict({"value": value}) == {"value": tuple(value)}
        assert FrozenJsonDict(value=value) == {"value": tuple(value)}
        assert type(FrozenJsonDict(value=value)["value"]) is tuple

    @pytest.mark.parametrize(
        "value",
        (
            {1: 1, 2: "2", 3: True, 4: (1, "a", True)},
            MappingProxyType({1: 1, 2: "2", 3: True, 4: (1, "a", True)}),
        ),
    )
    def test_convert_mapping_items(self, value):
        assert FrozenJsonDict({"value": value}) == {"value": value}
        assert FrozenJsonDict(value=value) == {"value": value}
        assert isinstance(FrozenJsonDict(value=value)["value"], FrozenJsonDict)

    def test_repr(self):
        obj = FrozenJsonDict(
            {
                "str": "abc",
                "int": 123,
                "bool": True,
                "null": None,
                "float": 123.45,
                "dict": {
                    "list": ["a", 1, None],
                    "set": {1, 2, 3},
                    "tuple": ("a", 1, None, dt.date(2023, 11, 24)),
                    "frozenset": {1, 2, 3},
                    "datatime": dt.datetime(
                        2023, 11, 23, 10, 11, 12, 789, dt.timezone.utc
                    ),
                },
                "uuid": UUID(int=1),
                "decimal": Decimal("0.01"),
            }
        )
        assert repr(obj) == (
            "{'str': 'abc', 'int': 123, 'bool': True, 'null': None, 'float': 123.45, "
            "'dict': {'list': ('a', 1, None), 'set': (1, 2, 3), 'tuple': ('a', 1, None, "
            "'2023-11-24'), 'frozenset': (1, 2, 3), 'datatime': "
            "'2023-11-23T10:11:12.000789+00:00'}, 'uuid': "
            "'00000000-0000-0000-0000-000000000001', 'decimal': '0.01'}"
        )

    def test_str(self):
        obj = FrozenJsonDict(
            {
                "str": "abc",
                "int": 123,
                "bool": True,
                "null": None,
                "float": 123.45,
                "dict": {
                    "list": ["a", 1, None],
                    "set": {1, 2, 3},
                    "tuple": ("a", 1, None, dt.date(2023, 11, 24)),
                    "frozenset": {1, 2, 3},
                    "datatime": dt.datetime(
                        2023, 11, 23, 10, 11, 12, 789, dt.timezone.utc
                    ),
                },
                "uuid": UUID(int=1),
                "decimal": Decimal("0.01"),
            }
        )
        assert str(obj) == (
            '{"str": "abc", "int": 123, "bool": true, "null": null, "float": 123.45, '
            '"dict": {'
            '"list": ["a", 1, null], "set": [1, 2, 3], '
            '"tuple": ["a", 1, null, "2023-11-24"], '
            '"frozenset": [1, 2, 3], '
            '"datatime": "2023-11-23T10:11:12.000789+00:00"}, '
            '"uuid": "00000000-0000-0000-0000-000000000001", '
            '"decimal": "0.01"}'
        )

    @pytest.mark.filterwarnings("ignore:Method not implemented")
    def test_fail_set_item(self):
        obj = FrozenJsonDict()

        with pytest.raises(NotImplementedError):
            obj["value"] = 123

    @pytest.mark.filterwarnings("ignore:Method not implemented")
    def test_fail_update(self):
        obj = FrozenJsonDict()
        assert obj == {}

        with pytest.raises(NotImplementedError):
            obj.update({"a": 1})

        assert obj == {}

    @pytest.mark.filterwarnings("ignore:Method not implemented")
    def test_fail_pop(self):
        obj = FrozenJsonDict(a=1)
        assert obj == {"a": 1}

        with pytest.raises(NotImplementedError):
            obj.pop("a")

        assert obj == {"a": 1}

    @pytest.mark.filterwarnings("ignore:Method not implemented")
    def test_fail_popitem(self):
        obj = FrozenJsonDict(a=1)
        assert obj == {"a": 1}

        with pytest.raises(NotImplementedError):
            obj.popitem()

        assert obj == {"a": 1}

    @pytest.mark.filterwarnings("ignore:Method not implemented")
    def test_fail_set_default(self):
        obj = FrozenJsonDict(a=1)
        assert obj == {"a": 1}

        with pytest.raises(NotImplementedError):
            _ = obj.setdefault("b", 2)

        assert obj == {"a": 1}

    @pytest.mark.filterwarnings("ignore:Method not implemented")
    def test_fail_delitem(self):
        obj = FrozenJsonDict(a=1)
        assert obj == {"a": 1}

        with pytest.raises(NotImplementedError):
            del obj["a"]

        assert obj == {"a": 1}

    @pytest.mark.filterwarnings("ignore:Method not implemented")
    def test_fail_clear(self):
        obj = FrozenJsonDict(a=1)
        assert obj == {"a": 1}

        with pytest.raises(NotImplementedError):
            obj.clear()

        assert obj == {"a": 1}

    def test_hash(self):
        obj = FrozenJsonDict(a=1)
        assert hash(obj) == hash('{"a": 1}')

        s = set()
        s.add(obj)
        assert s == {obj}
