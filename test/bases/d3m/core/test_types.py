import pytest
from d3m.core import DomainName, MessageName


class TestDomain:
    @pytest.mark.parametrize(
        "value, exception",
        [
            ("1", ValueError),
            ("CamelCase", ValueError),
            ("has_symbol", ValueError),
            ("-start-from-", ValueError),
            ("has-number-1", ValueError),
            ("test-domain--name.subdomain", ValueError),
            ("test-domain-name.subdomain--name", ValueError),
        ],
    )
    def test_fail_create(self, value, exception):
        with pytest.raises(exception, match="has not allowed symbols"):
            DomainName(value)

    def test_success_create(self):
        assert isinstance(DomainName("test"), DomainName)
        assert DomainName("test-domain-name") == "test-domain-name"
        assert (
            DomainName("test-domain-name.subdomain-name")
            == "test-domain-name.subdomain-name"
        )

    def test_properties(self):
        dname = DomainName("test-domain-name")
        assert dname.part_of is None
        dname = DomainName("test-domain-name.subdomain-name")
        assert dname.part_of == "test-domain-name"
        assert isinstance(dname.part_of, DomainName)

        dname = DomainName("test-domain-name.subdomain-name.subsubdomain-name")

        assert dname.part_of == "test-domain-name.subdomain-name"
        assert dname.part_of.part_of == "test-domain-name"
        assert dname.part_of.part_of.part_of is None

    def test_not_create_new_instance_from_domainname_class(self):
        dn = DomainName("test-domain-name")
        assert dn is DomainName(dn)

    def test_repr(self):
        assert repr(DomainName("name")) == "DomainName('name')"

    def test_str(self):
        assert str(DomainName("name")) == "name"


class TestCommandsAndEventsName:
    @pytest.mark.parametrize(
        "value, exception",
        [
            ("1", ValueError),
            ("startFromLowCase", ValueError),
            ("has_symbol_", ValueError),
            ("has-symbol-", ValueError),
            ("1HasNumber", ValueError),
            ("AmenoDorime________", ValueError),
        ],
    )
    def test_fail_create(self, value, exception):
        with pytest.raises(exception):
            MessageName(value)

    @pytest.mark.parametrize("value", ["TestEventName", "TestEventName1", "N1a2m3e4"])
    def test_success_create(self, value):
        assert MessageName(value) == value

    @pytest.mark.parametrize("value", ["TestEventName", "TestEventName1", "N1a2m3e4"])
    def test_repr(self, value):
        assert repr(MessageName(value)) == f"MessageName({value!r})"

    @pytest.mark.parametrize("value", ["TestEventName", "TestEventName1", "N1a2m3e4"])
    def test_str(self, value):
        assert str(MessageName(value)) == value
