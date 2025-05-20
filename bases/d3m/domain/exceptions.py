import inspect
from contextlib import suppress
from string import Formatter
from typing import Mapping, Any

from pydantic_core import to_jsonable_python

from d3m.core import DomainName
from .bases import get_domain_name
from .collection import DomainObjectsCollection


def _get_template_parameters(template: str) -> tuple[str, ...]:
    result = []
    for _, name, _, _ in Formatter().parse(template):
        if name is None:
            continue
        if not name.isidentifier():
            raise ValueError(f"invalid template parameter name {name!r}")
        result.append(name)
    return tuple(result)


class _DomainErrorMeta(type):
    def __init__(cls, name, bases, namespace, domain: DomainName | str | None = None):
        super().__init__(name, bases, namespace, domain=domain)
        if cls.__module__ == __name__ and cls.__name__ == "DomainError":
            return
        domain = get_domain_name(cls, bases, domain)
        if domain is not None:
            cls.__domain_name = domain
        else:
            raise ValueError(
                f"required set domain name for error '{cls.__module__}.{cls.__name__}'"
            )
        if (
            DomainError in bases
            and (base_class := _get_base_domain_error_class(cls.__domain_name__))
            is not None
        ):
            raise RuntimeError(
                f"base error class for domain '{cls.__domain_name__}' "
                f"already registered: {base_class.__module__}.{base_class.__qualname__}"
            )

        if hasattr(cls, "__template__"):
            cls.__init_parameters = _get_template_parameters(cls.__template__)

        DomainObjectsCollection().register(
            "error", cls.__domain_name__, cls.__name__, cls
        )

    @property
    def __domain_name__(cls) -> DomainName:
        return cls.__domain_name

    def validate_init_parameters(cls, parameters: dict):
        params = [param for param in cls.__init_parameters if param not in parameters]
        params_count = len(params)
        error_message = None
        if params_count == 1:
            error_message = (
                f"{cls.__module__}.{cls.__qualname__}() missing {params_count} "
                f"required keyword-only argument: {params[0]!r}"
            )
        elif params_count > 1:
            last_param = params[-1]
            other_params = ", ".join(map(repr, params[:-1]))
            error_message = (
                f"{cls.__module__}.{cls.__qualname__}() missing {params_count} "
                f"required keyword-only arguments: {other_params} and {last_param!r}"
            )

        if error_message is not None:
            raise TypeError(error_message)


class DomainError(Exception, metaclass=_DomainErrorMeta):
    """
    Custom Exception class for domain-specific errors

    It allows for the definition of specific error messages and
    supports dynamic parameter substitution in
    the error message template.

    Attributes:
        __template__ (str):
        The template string used for formatting the error message.
        All attributes in template required for init error.
        __domain_name__ (DomainName): readonly attribute retrieves the domain name associated with the current class.
        __payload__ (Mapping[str, Any]): readonly attribute retrieves the payload (additional parameters) associated with the error.

    Examples:
        **Creating a custom DomainError subclass**
        >>> class MyDomainError(DomainError, domain='my-domain'):
        ...     __template__ = "An error occurred in domain '{domain}'."
        >>> assert MyDomainError.__domain_name__ == DomainName('my-domain')

        **Generating error message use template**
        >>> MyDomainError(domain='my-domain', foo='bar')
        MyDomainError("An error occurred in domain 'test'.")

        **Get initialization error attribures**
        >>> MyDomainError(domain='my-domain', foo='bar').__payload__
        {'domain': 'my-domain', 'foo': 'bar'}

        **Fail init error without template attributes**
        >>> MyDomainError(foo='bar')
        TypeError: MyDomainError() missing 1 required keyword-only argument: 'domain'

        **Init with custom message, ignore template**
        >>> error = MyDomainError('custom message')
        >>> error
        MyDomainError('custom message')
        >>> error.__payload__
        {}
    """

    __template__: str

    def __init_subclass__(cls, domain: DomainName | str | None = None, **kwargs):
        super().__init_subclass__(**kwargs)

    def __init__(self, __message: str | None = None, /, **kwargs):
        if __message is None:
            self.__class__.validate_init_parameters(kwargs)
            message = self.__template__.format_map(kwargs)
        else:
            message = __message
        super().__init__(message)
        self._payload = to_jsonable_python(kwargs, serialize_unknown=True)

    @property
    def __domain_name__(self) -> DomainName:
        return self.__class__.__domain_name__  # type: ignore

    @property
    def __payload__(self) -> Mapping[str, Any]:
        return self._payload


def get_error_class(domain: str | DomainName, name: str) -> type[DomainError]:
    """
    Return registered error class by domain and name

    Attributes:
        domain (DomainName | str): The domain of the error.
        name (str): The name of the error.

    Returns:
        The class of the error.

    Examples:
        >>> from d3m.domain import DomainError
        >>> class PersonNotFound(DomainError, domain='person')
        ...     __template__: str = 'Person {reference} not found'
        >>> error_class = get_error_class('person', 'PersonNotFound')
        >>> assert error_class is PersonNotFound
    """
    return DomainObjectsCollection().get_domain_object("error", domain, name)


def _get_base_domain_error_class(domain: str | DomainName) -> type[DomainError] | None:
    classes = DomainObjectsCollection().get_domain_objects("error", domain)
    return next((klass for klass in classes if DomainError in klass.__bases__), None)  # type: ignore


def _create_error_class(
    domain: str | DomainName,
    name: str,
    module: str,
    bases=(DomainError,),
    template: str | None = None,
) -> type[DomainError]:
    namespace = {"__module__": module}
    if template is not None:
        namespace["__template__"] = template
    return type(name, bases, namespace, domain=domain)  # type: ignore


def get_or_create_base_error_class(
    domain: str | DomainName, *, template: str | None = None
) -> type[DomainError]:
    """
    Get or create a base error class associated with the given domain

    Attributes:
        domain (str | DomainName): The domain of the base error class.
        template (str | None): The template for the base error class. Defaults to None.
    Returns:
        The base error class associated with the given domain, or a newly created base error class if one does not exist.

    Examples:
        **Get existing base error class**
        >>> class BasePersonDomainError(DomainError, domain='person'):
        ...     pass
        >>> class PersonNotFound(BasePersonDomainError):
        ...     __template__ = 'Person {reference} not found'
        >>> error_class = get_or_create_base_error_class('person')
        >>> assert error_class is BasePersonDomainError

        **Create new base error class**
        >>> error_class = get_or_create_base_error_class('person')
        >>> assert error_class.__module__ is __name__
        >>> error_class.__name__
        '__BaseError__'
        >>> error_class.__domain_name__
        DomainName('person')
    """
    mod = inspect.getmodule(inspect.stack()[1].frame)
    module = mod.__name__ if mod else __name__
    return _get_or_create_base_error_class(domain, module=module, template=template)


def _get_or_create_base_error_class(
    domain: str | DomainName,
    *,
    template: str | None = None,
    module: str,
) -> type[DomainError]:
    base = _get_base_domain_error_class(domain)
    if base is not None:
        return base
    return _create_error_class(domain, "__BaseError__", module, template=template)


def get_or_create_error_class(
    domain: str | DomainName, name: str, template: str | None = None
) -> type[DomainError]:
    """
    Get or create a error class associated with the given domain.
    If base error class associated with the given domain does not exist, then create it.

    Attributes:
        domain (str | DomainName): The domain of the error class.
        template (str | None): The template for the error class. Defaults to None.

    Returns:
        A type object representing the error class for the given domain and name.

    Examples:
        **Get existing error class**
        >>> class BasePersonDomainError(DomainError, domain='person'):
        ...     pass
        >>> class PersonNotFound(BasePersonDomainError):
        ...     __template__ = 'Person {reference} not found'
        >>> error_class = get_or_create_error_class(
        ...     domain='person',
        ...     name='PersonNotFound',
        ... )
        >>> assert error_class is PersonNotFound

        **Create new error class**
        >>> class BasePersonDomainError(DomainError, domain='person'):
        ...     pass
        >>> error_class = get_or_create_error_class(
        ...     domain='person',
        ...     name='PersonNotFound',
        ...     template='Person {reference} not found'
        ... )
        >>> assert error_class.__name__ == 'PersonNotFound'
        >>> assert error_class.__domain_name__ = 'person'
        >>> assert isinstance(error_class, BasePersonDomainError)
    """
    with suppress(RuntimeError):
        return get_error_class(domain, name)

    mod = inspect.getmodule(inspect.stack()[1].frame)
    module = mod.__name__ if mod else __name__
    base = _get_or_create_base_error_class(domain, template=None, module=module)
    return _create_error_class(domain, name, module, (base,), template)
