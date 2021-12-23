import pytest

from rel2graph.relational_modules.pandas import PandasSeriesResource, PandasDataframeIterator
import pandas as pd

@pytest.fixture
def example_dataframe():
    data = {
    "ID": [1,2,2,3,4,4],
    "FirstName": ["Julian", "Fritz",  "Fritz", "Hans", "Rudolfo", "Rudolfo"],
    "LastName": ["Minder", "Generic", "SomeGuy", "Müller", "Muster", "Muster"],
    "FavoriteFlower": ["virginica", "setosa", "setosa", "versicolor", "setosa", "setosa"]
    }
    return pd.DataFrame(data)

@pytest.fixture
def example_series(example_dataframe):
    return example_dataframe.iloc[0]
    
@pytest.fixture
def resource(example_series):
    return PandasSeriesResource(series=example_series, type="ExampleType")

class TestPandasSeriesResource:
    def test_attributes(self, resource, example_series):
        # Test attributes
        assert resource.type == "ExampleType"
        assert (resource.series == example_series).all()
    
    def test_getitem(self, resource, example_series):
        # Test get item
        for key, value in example_series.iteritems():
            assert resource[key] == value

    def test_setitem(self, resource, example_series):
        # existing item
        resource[example_series.index[0]] = "Changed"
        assert resource[example_series.index[0]] == "Changed"
        # not existing item
        resource["NotExisting"] = "SomeValue"
        assert resource["NotExisting"] == "SomeValue"
    
    def test_repr(self, resource, example_series):
        assert str(resource) == f"PandasSeriesResource 'ExampleType' (row {example_series.name})"

class TestPandasDataframeIterator:
    @pytest.fixture
    def iterator(self, example_dataframe):
        return PandasDataframeIterator(example_dataframe, type="ExampleType")

    def compare_resources(self, resource1, resource2):
        return str(resource1) == str(resource2) and (resource2.series == resource1.series).all()

    def test_len(self, iterator, example_dataframe):
        assert len(iterator) == len(example_dataframe)

    def test_first(self, iterator, resource):
        first_resource = iterator.next()
        assert first_resource.series.name == 0
        assert self.compare_resources(first_resource, resource)

    def test_next(self, iterator, resource):
        first_resource = iterator.next()
        second_resource = iterator.next()
        assert not self.compare_resources(first_resource, second_resource)
        assert second_resource.series.name == 1
    
    def test_last(self, iterator):
        ret = iterator.next()
        for i in range(6):
            assert ret is not None
            ret = iterator.next()
        assert ret is None

    def test_reset_to_first(self, iterator, resource):
        for i in range(7):
            iterator.next()
        iterator.reset_to_first()
        assert self.compare_resources(iterator.next(), resource)
