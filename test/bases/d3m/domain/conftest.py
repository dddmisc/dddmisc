import pytest

from d3m.domain.collection import DomainObjectsCollection


@pytest.fixture(autouse=True)
def clear_domain_objects_collection():
    coll_collections = DomainObjectsCollection()._collections
    coll_register = DomainObjectsCollection()._registered_classes
    DomainObjectsCollection()._collections = {}
    DomainObjectsCollection()._registered_classes = set()
    yield
    DomainObjectsCollection()._collections = coll_collections
    DomainObjectsCollection()._registered_classes = coll_register
