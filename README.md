[![Tests Neo4j 5.13](https://github.com/jkminder/data2neo/actions/workflows/tests_neo4j5.yaml/badge.svg)](https://github.com/jkminder/data2neo/actions/workflows/tests_neo4j5.yaml)
[![Python Versions](https://img.shields.io/badge/python-3.8%20%7C%C2%A03.9%C2%A0%7C%C2%A03.10%C2%A0%7C%203.11%C2%A0%7C%203.12-orange)](https://github.com/jkminder/data2neo/actions/workflows) 

---
<p align="center">
  <img src="docs/source/assets/images/data2neo_banner.png" alt="Data2Neo banner"/>
</p>

---
**Data2Neo** is a library that simplifies the convertion of data in relational format to a graph knowledge database. It reliefs you of the cumbersome manual work of writing the conversion code and let's you focus on the conversion schema and data processing.

The library is built specifically for converting data into a [neo4j](https://neo4j.com/) graph (minimum version 5.2). The library further supports extensive customization capabilities to clean and remodel data. As neo4j python client it uses the native [neo4j python client](https://neo4j.com/docs/getting-started/languages-guides/neo4j-python/).


 - [Latest Releases](https://github.com/jkminder/data2neo/tags)
 - [Documentation](https://Data2Neo.jkminder.ch)

This library has been developed at the [Chair of Systems Design at ETH Zürich](https://www.sg.ethz.ch).

## Installation
```
pip install data2neo
```
The Data2Neo library supports Python 3.8+.

## Quick Start
A quick example for converting data in a [Pandas](https://pandas.pydata.org) dataframe into a graph. The full example code can be found under [examples](/examples). For more details, please checkout the [full documentation][wiki]. We first define a *convertion schema* in a YAML style config file. In this config file we specify, which entites are converted into which nodes and which relationships. 
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
    RELATIONSHIP(flower, "is", species):
    
ENTITY("Person"):
    NODE("Person") person:
        + ID = Person.ID
        - FirstName = Person.FirstName
        - LastName = Person.LastName
    RELATIONSHIP(person, "likes", MATCH("Species", Name=Person.FavoriteFlower)):
        - Since = "4ever"
```
The library itself has 2 basic elements, that are required for the conversion: the `Converter` that handles the conversion itself and an `Iterator` that iterates over the relational data. The iterator can be implemented for arbitrary data in relational format. Data2Neo currently has preimplemented iterators under:
- `Data2Neo.relational_modules.sqlite`  for [SQLite](https://www.sqlite.org/index.html) databases
- `Data2Neo.relational_modules.pandas` for [Pandas](https://pandas.pydata.org) dataframes

We will use the `PandasDataFrameIterator` from `Data2Neo.relational_modules.pandas`. Further we will use the `IteratorIterator` that can wrap multiple iterators to handle multiple dataframes. Since a pandas dataframe has no type/table name associated, we need to specify the name when creating a `PandasDataFrameIterator`. We also define define a custom function `append` that can be refered to in the schema file and that appends a string to the attribute value. For an entity with `Flower["petal_width"] = 5`, the outputed node will have the attribute `petal_width = "5 milimeters"`.
```python
import neo4j
import pandas as pd 
from data2neo.relational_modules.pandas import PandasDataFrameIterator 
from data2neo import IteratorIterator, Converter, Attribute, register_attribute_postprocessor
from data2neo.utils import load_file

# Setup the neo4j uri and credentials
uri = "bolt:localhost:7687"
auth = neo4j.basic_auth("neo4j", "password")

people = ... # a dataframe with peoples data (ID, FirstName, LastName, FavoriteFlower)
people_iterator = PandasDataFrameIterator(people, "Person")
iris = ... # a dataframe with the iris dataset
iris_iterator = PandasDataFrameIterator(iris, "Flower")

# register a custom data processing function
@register_attribute_postprocessor
def append(attribute, append_string):
    new_attribute = Attribute(attribute.key, attribute.value + append_string)
    return new_attribute

# Create IteratorIterator
iterator = IteratorIterator([pandas_iterator, iris_iterator])

# Create converter instance with schema, the final iterator and the graph
converter = Converter(load_file("schema.yaml"), iterator, uri, auth)
# Start the conversion
converter()
```
# Known issues
If you encounter a bug or an unexplainable behavior, please check the [known issues](https://github.com/jkminder/Data2Neo/labels/bug) list. If your issue is not found, submit a new one.

[wiki]: https://data2neo.jkminder.ch/index.html
