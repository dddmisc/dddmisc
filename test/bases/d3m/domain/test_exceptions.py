import pytest

from d3m.core import DomainName
from d3m.core.types import FrozenJsonDict
from d3m.domain import (
    DomainError,
    get_error_class,
    get_or_create_error_class,
    get_or_create_base_error_class,
)
from d3m.domain.collection import DomainObjectsCollection


class TestDomainException:
    def test_error_domain_name(self):
        class Error(DomainError, domain="test"):
            __template__ = ""

        assert isinstance(Error.__domain_name__, DomainName)
        assert Error.__domain_name__ == "test"
        assert Error().__domain_name__ == "test"

    def test_inherit_domain_name(self):
        class Error(DomainError, domain="test"):
            __template__ = ""

        class Error2(Error):
            pass

        assert Error2.__domain_name__ is Error.__domain_name__
        assert Error2().__domain_name__ is Error.__domain_name__

    def test_replace_domain_name_in_inheritance_class(self):
        class Error1(DomainError, domain="test"):
            pass

        with pytest.raises(
            RuntimeError,
            match=r"not allowed replace domain name in child class\: domain\.test_exceptions\.Error2",
        ):

            class Error2(Error1, domain="test1"):
                pass

    def test_fail_create_error_class_without_domain(self):
        with pytest.raises(
            ValueError, match=f"required set domain name for error '{__name__}.Error'"
        ):

            class Error(DomainError):
                pass

    def test_template_without_parameters(self):
        class Error(DomainError, domain="test"):
            __template__ = "Test error message"

        assert str(Error()) == "Test error message"

    @pytest.mark.parametrize("template", ("{ref=}", "{0.ref}", "{ref[abc]}"))
    def test_fail_create_error_class_wint_invalid_template_parameter_name(
        self, template
    ):
        with pytest.raises(ValueError, match="invalid template parameter name"):

            class Error(DomainError, domain="test"):
                __template__ = template

    def test_not_use_template_if_message_set(self):
        class Error(DomainError, domain="test"):
            __template__ = "Test error {message}"

        assert str(Error("Other message")) == "Other message"

    def test_format_template(self):
        class Error(DomainError, domain="test"):
            __template__ = "arg1={arg1}, arg2={arg2}, arg3={arg3}"

        error = Error(arg1="abc", arg2=123, arg3=["a", 1, False], arg4="xyz")

        assert str(error) == "arg1=abc, arg2=123, arg3=['a', 1, False]"

    def test_fail_init_error_without_required_attributes(self):
        class Error(DomainError, domain="test"):
            __template__ = "arg1={arg1}, arg2={arg2}, arg3={arg3}"

        with pytest.raises(TypeError) as exc:
            Error()

        assert str(exc.value) == (
            f"{__name__}.{Error.__qualname__}() missing 3 required keyword-only "
            f"arguments: 'arg1', 'arg2' and 'arg3'"
        )

        with pytest.raises(TypeError) as exc:
            Error(arg1="abc")

        assert str(exc.value) == (
            f"{__name__}.{Error.__qualname__}() missing 2 required keyword-only "
            f"arguments: 'arg2' and 'arg3'"
        )

        with pytest.raises(TypeError) as exc:
            Error(arg1="abc", arg2=123)

        assert str(exc.value) == (
            f"{__name__}.{Error.__qualname__}() missing 1 required keyword-only "
            f"argument: 'arg3'"
        )

    def test_error_payload(self):
        class Error(DomainError, domain="test"):
            __template__ = "Test error message"

        error = Error(arg1="abc", arg2=123, arg3=False)

        assert error.__payload__ == dict(arg1="abc", arg2=123, arg3=False)

        assert isinstance(error.__payload__, FrozenJsonDict)

    def test_register_error_class_in_objects_collection(self):
        class Error(DomainError, domain="test"):
            pass

        classes = DomainObjectsCollection().get_domain_objects("error", "test")
        assert len(classes) == 1
        assert classes[0] is Error

    def test_get_error_class(self):
        class Error(DomainError, domain="test"):
            pass

        assert get_error_class("test", "Error") is Error

    def test_create_base_domain_error(self):
        Error = get_or_create_base_error_class("test", template="test template")

        assert Error.__domain_name__ == "test"
        assert Error.__name__ == "__BaseError__"
        assert Error.__module__ == "domain.test_exceptions"
        assert Error.__template__ == "test template"

    def test_get_base_domain_error(self):
        class Error(DomainError, domain="test"):
            pass

        assert get_or_create_base_error_class("test") is Error

    def test_build_domain_error_class(self):
        classes = DomainObjectsCollection().get_domain_objects("error", "test")
        assert len(classes) == 0

        Error = get_or_create_error_class("test", "Error", "test error template")

        BaseError = get_or_create_base_error_class("test")

        assert issubclass(Error, BaseError)
        assert str(Error()) == "test error template"
        assert get_or_create_error_class("test", "Error") is Error
        assert Error.__module__ == "domain.test_exceptions"
        assert Error.__name__ == "Error"

        classes = DomainObjectsCollection().get_domain_objects("error", "test")
        assert len(classes) == 2
        assert classes == (BaseError, Error)

    def test_fail_create_many_base_error_classes_for_domain(self):
        class Error1(DomainError, domain="test"):
            pass

        with pytest.raises(RuntimeError) as exc:

            class Error2(DomainError, domain="test"):
                pass

        assert str(exc.value) == (
            "base error class for domain 'test' already registered: "
            "domain.test_exceptions.TestDomainException."
            "test_fail_create_many_base_error_classes_for_domain.<locals>.Error1"
        )
