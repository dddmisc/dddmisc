from typing import TypeAlias

from d3m.core import DomainName

ObjectsCategory: TypeAlias = str
ObjectName: TypeAlias = str
CategoryCollection: TypeAlias = dict[DomainName, dict[ObjectName, type]]


class SingletonMetaclass(type):
    _instance: "SingletonMetaclass"

    def __call__(cls, *args, **kwargs):
        if not hasattr(cls, "_instance"):
            cls._instance = super().__call__(*args, **kwargs)
        return cls._instance


class DomainObjectsCollection(metaclass=SingletonMetaclass):
    def __init__(self):
        self._collections: dict[ObjectsCategory, CategoryCollection] = {}
        self._registered_classes: set[int] = set()

    def register(
        self,
        category: ObjectsCategory,
        domain: DomainName | str,
        name: ObjectName,
        klass: type,
    ):
        assert isinstance(
            klass, type
        ), "Allows registration of only classes, not instance"
        domain = DomainName(domain)
        category = category.lower()
        klass_id = id(klass)
        registered_class = self._get_class(category, domain, name)
        if klass_id in self._registered_classes and registered_class is not klass:
            raise RuntimeError(
                f"{klass!r} already registered in collection with other name"
            )
        if registered_class is not None and registered_class is not klass:
            raise RuntimeError(
                f'Other {category} class for "{domain}" domain with name "{name}" already registered'
            )
        self._get_domain_collection(category, domain)[name] = klass
        self._registered_classes.add(klass_id)

    def get_domain_objects(
        self, category: ObjectsCategory, domain: DomainName | str
    ) -> tuple[type, ...]:
        category = category.lower()
        domain = DomainName(domain)
        if (
            category not in self._collections
            or domain not in self._collections[category]
        ):
            return ()
        return tuple(self._collections[category][domain].values())  # type: ignore

    def get_domain_object(
        self, category: ObjectsCategory, domain: DomainName | str, name: ObjectName
    ):
        category = category.lower()
        domain = DomainName(domain)
        klass = self._get_class(category, domain, name)
        if klass is None:
            raise RuntimeError(
                f'{category} class for "{domain}" domain with name "{name}" not registered'
            )
        return klass

    def _get_domain_collection(
        self, category: ObjectsCategory, domain: DomainName
    ) -> dict[ObjectName, type]:
        return self._collections.setdefault(category, {}).setdefault(domain, {})

    def _get_class(
        self, category: ObjectsCategory, domain: DomainName, name: ObjectName
    ) -> type | None:
        return self._collections.get(category, {}).get(domain, {}).get(name, None)
