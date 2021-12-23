import pytest

from rel2graph.relational_modules.odata import ODataResource, ODataListIterator

class DemoEntity:
    class EntitySet:
        def __init__(self, type):
            self.name = type
        
    def __init__(self, firstname, lastname, personnumber, type):
        self.FirstName = firstname
        self.LastName = lastname
        self.PersonNumber = personnumber
        self.entity_set = DemoEntity.EntitySet(type)

    def __repr__(self):
        return "(OData Entity Representation)"
@pytest.fixture
def example_entity():
    return DemoEntity("Max", "Muster", 1, "Person")

@pytest.fixture
def example_entity_list(example_entity):
    return [example_entity, DemoEntity("Fritz", "Helfer", 2, "Person")]
    
@pytest.fixture
def resource(example_entity):
    return ODataResource(example_entity)


class TestODataResource:
    def test_attributes(self, resource, example_entity):
        # Test attributes
        assert resource.type == "Person"
        assert resource.odata_entity.FirstName == example_entity.FirstName
        assert resource.odata_entity.LastName == example_entity.LastName
        assert resource.odata_entity.PersonNumber == example_entity.PersonNumber

    def test_getitem(self, resource):
        # Test get item
        for key, value in [("FirstName", "Max"), ("LastName", "Muster"), ("PersonNumber", 1)]:
            assert resource[key] == value

    def test_setitem(self, resource):
        # existing item
        resource["FirstName"] = "Changed"
        assert resource["FirstName"] == "Changed"
        # not existing item
        resource["NotExisting"] = "SomeValue"
        assert resource["NotExisting"] == "SomeValue"
    
    def test_repr(self, resource):
        assert str(resource) == "ODataResource 'Person' (OData Entity Representation)"

class TestOdataListIterator:
    def compare_resources(self, resource1, resource2):
        return resource1["FirstName"] == resource2["FirstName"]  and resource1["LastName"] == resource2["LastName"] \
            and resource1["PersonNumber"] == resource2["PersonNumber"] \
            and resource1.type == resource2.type
    
    @pytest.fixture
    def iterator(self, example_entity_list):
        return ODataListIterator(example_entity_list)

    def test_len(self, iterator):
        assert len(iterator) == 2

    def test_first(self, iterator, resource):
        first_resource = iterator.next()
        assert self.compare_resources(first_resource, resource)

    def test_next(self, iterator):
        first_resource = iterator.next()
        second_resource = iterator.next()
        assert not self.compare_resources(first_resource, second_resource)
        assert second_resource["PersonNumber"] == 2
    
    def test_last(self, iterator):
        ret = iterator.next()
        for i in range(2):
            assert ret is not None
            ret = iterator.next()
        assert ret is None

    def test_reset_to_first(self, iterator, resource):
        for i in range(2):
            iterator.next()
        iterator.reset_to_first()
        assert self.compare_resources(iterator.next(), resource)
