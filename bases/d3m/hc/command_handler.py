import abc
import inspect
from collections import ChainMap
from copy import copy
from types import UnionType
from typing import (
    Callable,
    ParamSpecKwargs,
    Coroutine,
    Any,
    get_origin,
    Union,
    get_args,
)

from d3m.core import (
    MessageType,
    AbstractCommand,
)
from d3m.core.abstractions import AbstractCommandMeta


class ICommandHandler(abc.ABC):
    @property
    @abc.abstractmethod
    def command_class(self) -> AbstractCommandMeta:
        """
        Property return command class for this handler
        Returns:

        """

    @property
    @abc.abstractmethod
    def origin_func(self) -> Callable[..., Coroutine] | None:
        """
        Property return origin function decorated by this handler
        Returns:

        """

    @abc.abstractmethod
    def with_command(self, command: AbstractCommand) -> "ICommandHandler":
        """
        Create copy of handler with set command for exec handler
        Args:
            command: Instance of command

        Returns:

        """

    @abc.abstractmethod
    def with_defaults(self, **defaults) -> "ICommandHandler":
        """
        Create copy of handler with set defaults context for exec handler
        Args:
            **defaults: dict of default values
        Returns:

        """

    @abc.abstractmethod
    async def __call__(self) -> Any:
        pass


class CommandHandler(ICommandHandler):
    def __init__(self):
        self._func: Callable[[ParamSpecKwargs], Coroutine] | None = None
        self._signature: inspect.Signature | None = None
        self._command_parameter: inspect.Parameter | None = None
        self._command: AbstractCommand | None = None
        self._defaults = {}

    @property
    def command_class(self) -> AbstractCommandMeta:
        if self._command_parameter is not None:
            return self._command_parameter.annotation
        raise AttributeError

    @property
    def origin_func(self) -> Callable[[ParamSpecKwargs], Coroutine] | None:
        return self._func

    async def __call__(self) -> Any:
        self._check_set_function()
        if self._command is None:
            raise RuntimeError("Required set command before execute handler")
        attrs = ChainMap(
            {self._command_parameter.name: self._command},  # type: ignore
            self._defaults,
        )
        result = await self._func(**attrs)  # type: ignore
        return result

    def set_function(self, func: Callable[[ParamSpecKwargs], Coroutine]):
        if self._func is not None:
            raise RuntimeError("function already set")
        signature = inspect.signature(func)
        self._validate_signature(signature)
        self._command_parameter = self._get_command_parameter(signature)
        self._func = func
        self._signature = self._normalize_generic_annotation(signature)
        self._defaults = self._get_defaults_from_signature(signature)
        return self

    def with_command(self, command: AbstractCommand):
        self._check_set_function()
        if self._command is not None:
            raise RuntimeError("Command context already set")
        elif not isinstance(command, self.command_class):
            raise TypeError(
                f"Invalid command type, expected {self.command_class!r} got {type(command)!r}"
            )

        handler = copy(self)
        setattr(handler, "_command", command)
        return handler

    def with_defaults(self, **defaults):
        if len(defaults) == 0:
            return self
        self._check_set_function()
        handler = copy(self)
        setattr(
            handler,
            "_defaults",
            dict(
                ChainMap(self._filter_arguments_by_signature(defaults), self._defaults)
            ),
        )
        return handler

    @staticmethod
    def _get_command_parameter(sign: inspect.Signature) -> inspect.Parameter:
        command_params = [
            param
            for param in sign.parameters.values()
            if get_origin(param.annotation) is None
            and isinstance(param.annotation, AbstractCommandMeta)
            and param.annotation.__type__ == MessageType.COMMAND
        ]
        count_params = len(command_params)
        if count_params == 1:
            return command_params[0]
        if count_params == 0:
            raise AttributeError(
                "Handler function required has AbstractCommand annotated parameter"
            )
        raise AttributeError(
            f"Expected one AbstractCommand annotated parameter got {count_params}"
        )

    @staticmethod
    def _validate_signature(sign: inspect.Signature):
        for param in sign.parameters.values():
            if param.kind == param.POSITIONAL_ONLY:
                raise AttributeError(
                    "Not allowed use function with positional only arguments as handler"
                )
            elif param.kind == param.VAR_KEYWORD:
                raise AttributeError(
                    "Not allowed use function with var keyword arguments as handler"
                )
            elif param.kind == param.VAR_POSITIONAL:
                raise AttributeError(
                    "Not allowed use function with var positional arguments as handler"
                )
            if param.annotation is param.empty:
                raise AttributeError(
                    "All parameters for handler function required be annotated"
                )

    @staticmethod
    def _get_defaults_from_signature(signature: inspect.Signature):
        defaults = {}
        for param in signature.parameters.values():
            if param.default != param.empty:
                defaults[param.name] = param.default
        return defaults

    def _filter_arguments_by_signature(self, dependencies) -> dict[str, Any]:
        self._check_set_function()
        result = {}
        for param in self._signature.parameters.values():  # type: ignore
            if param.name in dependencies:
                value = dependencies[param.name]
                if not isinstance(value, param.annotation):
                    raise TypeError(
                        f'Invalid type of "{param.name}" argument, '
                        f"expected {param.annotation!r} got {type(dependencies[param.name])}"
                    )
                result[param.name] = value

        return result

    def _check_set_function(self):
        if (
            self._func is None
            or self._command_parameter is None
            or self._signature is None
        ):
            raise RuntimeError(
                'Required call "set_function" before exec current method'
            )

    @staticmethod
    def _normalize_generic_annotation(
        signature: inspect.Signature
    ) -> inspect.Signature:
        parameters = []
        for key, parameter in signature.parameters.items():
            origin = get_origin(parameter.annotation)
            if origin is None:
                parameters.append(parameter)
            elif origin in (Union, UnionType):
                parameters.append(
                    parameter.replace(annotation=get_args(parameter.annotation))
                )
            else:
                parameters.append(parameter.replace(annotation=origin))

        return signature.replace(parameters=parameters)
