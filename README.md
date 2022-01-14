# Rel2graph

**Rel2graph** is a library that simplifies the convertion of data in relational format to a graph knowledge database. It reliefs you of the cumbersome manual work of writing the conversion code and let's you focus on the conversion schema and data processing.

The library is built specifically for converting data into a [neo4j](https://neo4j.com/) graph. The library further supports extensive customization capabilities to clean and remodel data. As neo4j python client it uses the [py2neo](https://py2neo.org/2021.1/index.html) library.


 - [Latest Releases](https://github.com/sg-dev/rel2graph/tags)
 - [Documentation][wiki]
 - [Developer Interface](docs/api.md)

Note: The py2neo library does not support parallel relations of the same type (same source, same target and same type). If your graph requires such parallel relations please checkout the provided [py2neo extensions](/docs/documentation.md#py2neo-extensions).
## Installation
If you have setup a private ssh key for your github, copy-paste the command below to install the latest version ([v0.2.2][latest_tag]):
```
$ pip install git+ssh://git@github.com/sg-dev/rel2graph@v0.2.2
```

If you don't have ssh set up, download the latest wheel [here][latest_wheel] and install the wheel with:
```
$ pip install **path-to-wheel**
```

If you have cloned the repository you can also build it locally with
```
$ pip install **path-to-repository**
```
The rel2graph libary supports Python 3.7+.

## Quick Start
A quick example for converting data in a [Pandas](https://pandas.pydata.org) dataframe into a graph. The full example code can be found under [examples](/examples). For more details, please checkout the [full documentation][wiki] (coming soon, I'm working on it :D ). We first define a *convertion schema* in a YAML style config file. In this config file we specify, which entites are converted into which nodes and which relations. 
##### **`schema.yaml`**
```yaml
ENTITY("Flower"):
    NODE("Flower") flower:
        - sepal_length = Flower.sepal_length
        - sepal_width = Flower.sepal_width
        - petal_length = Flower.petal_width
        - petal_width = append(Flower.petal_width, " milimeters")
    NODE("Species", "BioEntity") species:
        + Name = Flower.species
    RELATION(flower, "is", species):
    
ENTITY("Person"):
    NODE("Person") person:
        + ID = Person.ID
        - FirstName = Person.FirstName
        - LastName = Person.LastName
    RELATION(person, "likes", MATCH("Species", Name=Person.FavoriteFlower)):
        - Since = "4ever"
```
The library itself has 2 basic elements, that are required for the conversion: the `Converter` that handles the conversion itself and an `Iterator` that iterates over the relational data. The iterator can be implemented for arbitrary data in relational format. Rel2graph currently has preimplemented iterators under:
- `rel2graph.relational_modules.odata`  for [OData](https://www.odata.org) databases (based on [pyodata](https://pyodata.readthedocs.io))
- `rel2graph.relational_modules.pandas` for [Pandas](https://pandas.pydata.org) dataframes

We will use the `PandasDataframeIterator` from `rel2graph.relational_modules.pandas`. Further we will use the `IteratorIterator` that can wrap multiple iterators to handle multiple dataframes. Since a pandas dataframe has no type/table name associated, we need to specify the name when creating a `PandasDataframeIterator`. We also define define a custom function `append` that can be refered to in the schema file and that appends a string to the attribute value. For an entity with `Flower["petal_width"] = 5`, the outputed node will have the attribute `petal_width = "5 milimeters"`.
```python
from py2neo import Graph
import pandas as pd 
from rel2graph.relational_modules.pandas import PandasDataframeIterator 
from rel2graph import IteratorIterator, Converter, Attribute, register_attribute_postprocessor

# Create a connection to the neo4j graph with the py2neo Graph object
graph = Graph(scheme="http", host="localhost", port=7474,  auth=('neo4j', 'password')) 

people = ... # a dataframe with peoples data (ID, FirstName, LastName, FavoriteFlower)
people_iterator = PandasDataframeIterator(people, "Person")
iris = ... # a dataframe with the iris dataset
iris_iterator = PandasDataframeIterator(iris, "Flower")

# register a custom data processing function
@register_attribute_postprocessor
def append(attribute, append_string):
    new_attribute = Attribute(attribute.key, attribute.value + append_string)
    return new_attribute

# Create IteratorIterator
iterator = IteratorIterator([pandas_iterator, iris_iterator])

# Create converter instance with schema, the final iterator and the graph
converter = Converter("schema.yaml", iterator, graph)
# Start the conversion
converter()
```

[latest_version]: v0.2.2
[latest_tag]: https://github.com/sg-dev/rel2graph/releases/tag/v0.2.2
[latest_wheel]: https://github.com/sg-dev/rel2graph/releases/download/v0.2.2/rel2graph-0.2.2-py3-none-any.whl
[wiki]: docs/documentation.md
