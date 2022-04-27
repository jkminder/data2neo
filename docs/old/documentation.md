# Documentation

This is the documentation for the *rel2graph* library. For developers intending to work on the library itself, please read the [Information for developers](#information-for-developers) chapter.

For a Quick Start Example please refer to the [ReadMe](../README.md).

## Contents

- [Introduction](#introduction) 
- [Converter](#converter)
    - [Statefullness](#statefullness)
    - [Data types](#data-types)
    - [Logging and progress monitoring](#logging-and-progress-monitoring)
- [Schema Syntax](#schema-syntax)
    - [Entity](#entity)
    - [Node](#node)
    - [Attributes](#attributes)
    - [Relation](#relation)
    - [Match](#match)
    - [Merging Nodes](#merging-nodes)
    - [Wrappers](#wrappers)
- [Customising Resource and ResourceIterators](#customising-resource-and-resourceiterators)
    - [Resource](#resource)
    - [ResourceIterator](#resource-iterator)
    - [Existing relational modules](#existing-relational-modules)
        - [Pandas](#pandas)
        - [OData](#odata)
    
- [Building your own Wrappers](#building-your-own-wrappers)
    - [Background](#background)
    - [Registering](#registering)
    - [Preprocessors](#preprocessors)
    - [Postprocessors](#postprocessors)
    - [Full Wrappers](#full-wrappers)
- [Common modules](#common-modules)
- [Py2neo Extensions](#py2neo-extensions)
    - [Graph with parallel relations](#graph-with-parallel-relations)
- [Information for developers](#information-for-developers)
    - [Testing](#testing)
</br>
</br>

**Rel2graph** is a library that simplifies the conversion of data in relational format to a graph knowledge database. It relieves you of the cumbersome manual work of writing the conversion code and lets you focus on the conversion schema and data processing.

The library is built specifically for converting data into a [neo4j](https://neo4j.com/) graph. The library further supports extensive customisation capabilities to clean and remodel data. As neo4j python client it uses the [py2neo](https://py2neo.org/2021.1/index.html) library.

Most classes `SomeClass` in the following documentation can be clicked to get a more detailed developer interface.

## Introduction
This chapter will give you an overview of how *rel2graph* works and a first intuition on how to interact with it. Details on how the individual parts of the library work can be found in later chapters. Simplified, the library works like a factory that converts an input, some relational data, into an output, a [neo4j](https://neo4j.com/) graph. The factory input is called a [`Resource`](api.md#Resource). A [`Resource`](api.md#Resource) can wrap any relational entity. For every supplied resource, the factory will produce a graph. We define a **conversion schema** ahead that specifies the "factory blueprints": what it produces and how. Once the factory is set up and knows the schema, we can keep supplying it with resources without writing more code. 

<img src="assets/images/factory.png" alt="drawing" width="800"/>

Since there might be different types of resources, we build a factory per resource type. One specifies all the "blueprints" for all the factories in thr **conversion schema** file. A [`Converter`](api.md#Converter), the main object of *rel2graph*, will take this file and construct all the factories based on your "blueprints". For a set of supplied resources the [`Converter`](api.md#Converter) will automatically select the correct factory, use it to produce a graph out of the resource and merge the produced graph with the full [neo4j](https://neo4j.com/) graph. We supply resources to the converter with a [`ResourceIterator`](api.md#ResourceIterator). This iterator keeps track of what the next resource to process is. The [`Resource`](api.md#Resource) and [`ResourceIterator`](api.md#ResourceIterator) classes can be fully customised. A simple version of it might just point to a specific element in a list of resources, as visualised in the image below. The [`Converter`](api.md#Converter) iteratively asks the [`ResourceIterator`](api.md#ResourceIterator) for the next resource until the iterator reports no more resources to process.

<img src="assets/images/overview.png" alt="drawing" width="800"/>

At the simplest, the library consists of the following 4 parts: 
- [`Converter`](api.md#Converter): handles all the factories and builds the graph.
- The **conversion schema**, specifying what is converted into what. 
- [`Resource`](api.md#Resource): A wrapped relational entity 
- [`ResourceIterator`](api.md#ResourceIterator): An iterator for the to-be-processed resources. You can also use the provided [`IteratorIterator`](api.md#iteratoriterator) to iterate over multiple iterators.

The next chapters will go into detail about these 4 parts. In later chapters, we will show you how you can insert your custom code into one of these factories by creating [Wrappers](#building-your-own-wrappers). Wrappers can apply a pre- and/or postprocessing to a factory. 

<img src="assets/images/wrapper.jpg" alt="drawing" width="600"/>

A wrapper behaves like a factory and can be wrapped into another wrapper. This allows you to insert arbitrary customisation into the conversion and adapt it to your use-case.
## Converter
The [`Converter`](api.md#Converter) handles the main conversion of the relational data. It is initialised with the *conversion schema filename*, the iterator and the graph. 
```python
from rel2graph import Converter

converter = Converter(config_filename, iterator, graph)
```
To start the conversion, one simply calls the object. It then iterates twice over the iterator: first to process all the nodes and, secondly, to create all relations. This makes sure that any node a relation refers to is already created first.
```python
converter()
```
The [`Converter`](api.md#Converter) can utilise **multithreading**. When initialising you can set the number of parallel workers. Each worker operates in its own thread. Be aware that the committing to the graph is often still serialized, since the semantics require this (e.g. nodes must be committed before any relation or when [merging nodes](#merging-nodes) all the nodes must be serially committed). So the primary use-case of using multiple workers is if your resources are utilising a network connection (e.g. remote database) or if you require a lot of [matching](#match) in the graph (matching is parallelised).
```python
converter = Converter(config_filename, iterator, graph, num_workers = 20)
```
**Attention:** If you enable more than 1 workers, ensure that all your [custom wrappers](#building-your-own-wrappers) support multithreading (add locks if necessary).

### Statefullness   
The converter is **stateful**. If you call the converter in a *jupyter notebook* and an exception is raised during the converter's execution (e.g. due to a KeyboardInterrupt, a network problem, an error in the conversion schema file), you can fix the problem and call the converter again. The converter will continue its work where it left off (the resource responsible for the exception, will be reconverted). **It is guaranteed that no resource is lost.**

The state of the converter is reset when *initialising it* or when *you change the underlying iterator* with `converter.iterator = new_iterator`. So if you don't want the stateful behaviour, you need to do one of these two things.
#### Example 1
You use OData Resources, and the network connection suddenly drops out.
In the first cell, you initially have created the converter object and called it.
```python
converter = Converter(config_filename, iterator, graph)
converter()
```
Now a `ConnectionException`is raised due to network problems. You can now fix the problem and then recall the converter in a new cell:
```python
converter()
```
The converter will just continue where it left off. 

#### Example 2
You have a small error in your **conversion schema** for a specific entity (e.g. a typo in an attribute key), which is not immediately a problem since you first process a lot of other entities. Again in the first cell, you initially have created the converter object and called it.
```python
converter = Converter(config_filename, iterator, graph)
converter()
```
Now, e.g. `KeyError `is raised since the attribute name was written slightly wrong. Instead of rerunning the whole conversion (which might take hours), you can fix the schema file and reload the schema file and recall the converter:
```python
converter.reload_config(config_filename)
converter()
```
The converter will just continue where it left off with the new *conversion schema*. 

### Data types
Neo4j supports the following datatypes: **Number** (int or float), **String**, **Boolean**, **Point** as well as **temporal types** (Date, Time, LocalTime, DateTime, LocalDateTime, Duration) ([more here](https://neo4j.com/docs/cypher-manual/current/syntax/values/)). The py2graph library does currently not support **Points**. For all other types it will keep the type of the input. So if your resource provides ints/floats it will commit them as ints/floats to the graph. If you require a specific conversion you need to create your own custom wrappers. For **temporal values** the library uses the datetime/date objects of the python [datetime](https://docs.python.org/3/library/datetime.html) library. If you want to commit a date(time) value to the graph make sure it is a date(time) object. All inputs that are not of type: [numbers.Number](https://docs.python.org/3/library/numbers.html) (includes int & float), str, bool, [date](https://docs.python.org/3/library/datetime.html), [datetime](https://docs.python.org/3/library/datetime.html) are converted to strings before beeing commited to neo4j.

For converting strings to datetime/date the library provides some predefined wrappers. See [here](#common-modules) for more details.

### Logging and progress monitoring
The whole rel2graph library uses the standard python [logging](https://docs.python.org/3/howto/logging.html) library. See an example of how to use it below. For more information, check out the [official documentation](https://docs.python.org/3/howto/logging.html).
```python
import logging

logger = logging.getLogger("rel2graph") # Get Logger
logger.setLevel(logging.DEBUG) # Set the log level to DEBUG
log_formatter = logging.Formatter("%(asctime)s [%(threadName)s]::[%(levelname)s]::%(filename)s: %(message)s") # Specify the format
console_handler = logging.StreamHandler() # Create console handler (will output directly to console)
console_handler.setFormatter(log_formatter) # Add formater to handler
logger.addHandler(console_handler) # add handler to logger

```
When calling a [`Converter`](api.md#Converter) instance you can provide a **progress bar** class, that will be used to display progress. The progress bar class must be an instance of the [tqdm](https://tqdm.github.io) progress bar (or behave like it). 
```python
from tqdm import tqdm
converter(progress_bar = tqdm)
```
You can use any of the tdqm progress bars. For example to monitor the progress via telegram you can use:
```python
from tqdm.contrib.telegram import tqdm
pb = lambda **kwargs: tqdm(token="yourtoken", chat_id="yourchatid", **kwargs) # Config your tokens
converter(progress_bar=pb)
```
## Schema Syntax

The **conversion schema** defines which relational entities are converted to which graph elements (nodes and relations). As seen in the [Introduction](introduction.md), the [`Converter`](api.md#Converter) expects resources as inputs. A resource is a wrapper around a relational entity. Each resource has a *type* associated with it that corresponds to the *type* of entity it wraps. The *type* must be defined for every [`Resource`](api.md#Resource) instance and accessible at `Resource.type`. In the schema file, we specify for each *type* what the [`Converter`](api.md#Converter) should do when it encounters this *type*. The schema allows for one-to-one, one-to-many and many-to-one (with the help of [merging](#merging-nodes)) conversions of relational entities to graph elements (nodes and relations).

Note that the **conversion schema** compiler that the [`Converter`](api.md#Converter) uses to parse the provided schema is limited and does little syntax and no semantic checking. Make sure that you write correct **conversion schema syntax**. Otherwise, problems or weird behaviour might arise during runtime.

We define the **conversion schema** in a schema file. The file follows a modified YAML schema syntax. We will now look at our example from the [Quick Start](../README.md). 
```yaml
ENTITY("Flower"):
    NODE("Flower") flower:
        - sepal_length  = Flower.sepal_length
        - sepal_width = Flower.sepal_width
        - petal_length = Flower.petal_length
        - petal_width = Flower.petal_width
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

### Entity

We define the entity *type* with `ENTITY(`*type*`):` following the definitions for the graph elements that this *type* should be converted to. A schema file can contain multiple *type* definitions. Under (in a YAML sense) an entity *type* definition, we can refer to attributes of the *type* with `type.attribute_name`. So if the table for our **"Flower"** entity looks as follows:

**Table "Flower"**
| sepal_length | sepal_width | petal_length | petal_width | species |
|--------------|-------------|--------------|-------------|---------|
| 3.1          | 2.3         | 1.2          | 1.6         | setosa  |
| 2.4          | 1.3         | 1.3          | 0.6         | setosa  |
| ...          | ...         | ...          | ...         | ...     |

we can access the sepal_length attribute with `Flower.sepal_length`.

An entity can be converted into multiple graph elements: `NODE`s and `RELATION`s.

### Node
A node is defined with `NODE(`*label1, label2, ...*`)`; in between the brackets, we define the labels that the nodes should have. We can also define the label based on an attribute of the entity: `NODE(`*type.attributename*`)` (e.g. `NODE("Flower", Flower.species)` to add the species name as label to the node). After a node definition, we can specify its internal **identifier**. The identifier is optional and is used for referring to this node when creating relations, as we will see later. The **identifier** is only valid within one entity and, therefore must be unique per entity. The full syntax for defining a node is:
`NODE(`*`label1, label2, ...`*`) `**`identifier`**`:`

### Attributes
We can define the attributes of a node or a relation under it as follows (indented following YAML format):
`-`*`attribute_name`*`=`*`type.entity_attribute_name`*
Going back to our example, if the node with identifier **flower** should have an attribute named `sepal_length` that contains the value of the attribute `sepal_length` of the entity "Flower", we write 
`- sepal_length  = Flower.sepal_length`.
The attribute name of the node/relation must not be the same as the one of the entity. We could also do 
`- sl  = Flower.sepal_length` 
to get a node with attribute `sl`.

We can also set static attribute values (*strings, ints, floats or bools (True/False)* ) with 
```
- a_static_string_attribute = "some string"
- a_static_bool = True
- a_static_int = 1
- a_static_flaot = 1.123
```
### Relation
A relation is declared with `RELATION(`*source node(s)*`,`*relation type*`,`*destination node(s)*`)`. The relation type is a simple string that represents the relation's name. This will create a relation on the kartesian product of the *source node(s)* and the *destination node(s)* (from all sources to all destinations). We have two options on how to set source and destination nodes: 
- Use a node identifier (note that it must appear above the relation declaration under the same entity). This allows us to set a single node. E.g. in our example, we have defined the two nodes with identifiers `flower` and `species`. We can now define a relation between those two with `RELATION(flower, "is", species)`.
- Use the `MATCH` keyword. With a matcher, we can query for arbitrary nodes in the graph. This is useful when the node we want to refer to is either from a different instance of the same entity or from an other entity (or already existing in the graph). A matcher can return single or multiple nodes. 

### Match
The match syntax works as follows: `MATCH(`label1`,` label2`,` ...`,`attribute1=value1`,`attribute2=value2`,`...`)`. We first specify the required labels of the searched node(s). We can define arbitrarily many labels, and the labels themselves can be extracted from the entity as we did with the nodes (e.g. `Flower.species`). We then define the conditions that the nodes we are looking for must meet by specifying which attributes have which values. We write *myattribute=myvalue*, meaning that any matched node must have the value *myvalue* for its attribute *myattribute*. Again the value can be extracted from the entity (e.g. `Name=Person.FavoriteFlower`). We can specify an arbitrary amount of conditions. 


### Merging Nodes
If we expect that a node is created multiple times and we want to ensure that it is only created once, we can specify a primary attribute. This would be the case for the node with identifier **species** in our example. If multiple rows of the "Flower" table contain the same entry for the "species" column, the converter will create a node for this species for each row. So for our example table, we would end up with at least two "setosa" nodes. However, what we want is only one node for each species present. 

For this purpose, we can specify a **primary attribute** and a **primary label** to merge a node with the graph. The **primary label** is always the first one mentioned, so we reformulate the node definition to `NODE(`**`primary_label`***`, label2, ...`*`)`. The **primary attribute** is set by replacing the `-` in the attribute definition with a `+`:
`+`*`attribute_name`*`=`*`type.entity_attribute_name`*
If the `Converter` detects that a *primary attribute* is set, it will only create a new node if **no** node with the same primary label and primary attribute exists in the graph. If the node already exists, it is updated, i.e. new attributes are added, and existing attributes are updated according to the specified values. 

We can also use this to create nodes with information from different entity types. For example, lets assume we had an entity "Person" and an entity "Employee", both of them containing a per-person-unique property:

Table "Person"
| personId | name | ... |
|--------------|-------------|---------|
| ...          | ...         | ...     |


Table "Employee"
| personId | employer | ... |
|--------------|-------------|---------|
| ...          | ...         | ...     |

To create a node that contains both attributes of the entity "Person" and the entity "Employee" we can use the above-explained syntax to merge the nodes:
```yaml
ENTITY("Person"):
    NODE("Person"):
        + id = Person.personId
        - name  = Person.name
    
ENTITY("Employee"):
    NODE("Person"):
        + id = Employee.personId
        - employer  = Employee.employer
```
If you now supply both entities to the converter for every person the resulting nodes will have all the attributes `id`, `name` and `employer`. Note if you don't supply both entities for a person the node will only contain the information from the single entity that it got.

### Merging Relations
By default, py2neo merges all relations (between nodes *a* and *b* only one relation of type *type* can exist). If you require such parallel relations, use the `GraphWithParallelRelations` instead of the default py2neo graph; read more about it [here](#py2neo-extensions). If you use the `GraphWithParallelRelations` you can explicitly merge relations by specifying a **primary attribute**. The syntax is the same as for nodes:
```yaml
    RELATION(from, "type", to):
        + primary_attribute = Entity.ID
        - other_attribute = Entity.other
```
If you don't specify a primary attribute and two entities result in the same *from* and *to* node, two relations will be created in parallel.
### Wrappers
If you have registered wrappers (see [here](#building-your-own-wrappers)) you can refer to them in the **conversion schema**. You simply use the syntax `NameOfWrapper(`wrappedcontent`)`, similar to how you call a function. Find examples below.

Assuming you have defined the attributewrappers `ATTRWRAPPER1` and `ATTRWRAPPER2`, as well as the subgraphwrappers, `SGWRAPPER1` and `SGWRAPPER2`:
```yaml
    - name = ATTRWRAPPER2(ATTRWRAPPER1(Person.personId))
```
```yaml
RELATION(person, ATTRWRAPPER2("likes"), MATCH(ATTRWRAPPER1("Species"), Name=ATTRWRAPPER(Person.FavoriteFlower))):
```
```yaml
SGWRAPPER1(RELATION(person, "likes", MATCH(ATTRWRAPPER1("Species"), Name=ATTRWRAPPER(Person.FavoriteFlower)))):
```
```yaml
SGWRAPPER2(SGWRAPPER1(NODE("Flower"))):
```

Note that the library does no semantic checking of your schema. If you apply an attribute wrapper to a node or a relation, the outcome is undefined and might result in unexpected behaviour/exceptions during runtime.

## Customising Resource and ResourceIterator
The [`Resource`](api.md#Resource) and [`ResourceIterator`](api.md#ResourceIterator) classes are abstract classes and need to be implemented for your specific relational input data type. The library comes with implementations for a few [relational modules](#existing-relational-modules). If you data type is not supported, you can easily create implementations for your data; see below for an explanation or check out [one of the already implemented modules](./../rel2graph/relational_modules/pandas.py). 


If you think your implementations could be helpful for others as well, read through the chapter [Information for Developers](#information-for-developers) and create a pull request.
### Resource
The [`Resource`](api.md#Resource) is simply a wrapper around a relational entity. To create your own resource, you create a new class and inherit from [`Resource`](api.md#Resource). The parent constructor must be called with no arguments. Additionally, you need to implement following methods: `type`,`__getitem__`,`__setitem__`, `__repr__`. See the docs strings below for more details.
```python
from rel2graph import Resource
class MyResource(Resource):
    def __init__(...your constructor parameters...):
        super().__init__()
        # your constructor code
        ...

    @property
    def type(self) -> str:
        """
        Returns the type of the resource as a string. The @property decorator makes this a property of the resource. The type is then accessible via resource.type without braces.
        """
        ...

    def __getitem__(self, key: str) -> str:
        """
        Returns the value at key 'key' of the underlying relational entity. 
        Makes an item retrievable via resource["mykey"].
        """
        ...
    
    def __setitem(self, key: str, value: str) -> None:
        """
        Sets/Changes the value of the underlying relational entity at key 'key'. 
        It must be able to change existing key and also add new keys.
        Makes an item settable via resource["mykey"] = "somevalue".
        """
        ...

    def __repr__(self) -> str:
        """
        Gets a string representation of the resource. Only used for logging.

        Should follow the format:
        NameOfResource 'TypeOfResource' (DetailsAboutResource)
        
        super().__repr__() returns "NameOfResource 'TypeOfResource'"

        Example-Implementation:
        f"{super().__repr__()} ({self.somedetail})"
        """
        return f"{super().__repr__()} ({... some detail about your resource ...})"
    
```

### ResourceIterator
If you create your own [`Resource`](api.md#Resource), you also need to create a [`ResourceIterator`](api.md#ResourceIterator) for this [`Resource`](api.md#Resource) that allows the [`Converter`](api.md#Converter) to iterate over a set of resources. The iterator needs to be written such that it can be traversed multiple times (The [`Converter`](api.md#Converter) will traverse it once for all nodes and then again for all relations). 

Your iterator class must inherit from [`ResourceIterator`](api.md#resourceiterator), and its constructor must call the parent constructor with no arguments. Additionally, you need to implement following methods: `next`,`reset_to_first`,`__len__`. See the docs strings below for more details. Note that your iterator can also traverse resources of different types.

```python
from rel2graph import ResourceIterator
class MyIterator(ResourceIterator):
    def __init__(self, ...your constructor parameters...) -> None:
        super().__init__()
        ...

    def next(self) -> Resource:
        """Gets the next resource that will be converted. Returns None if the range is traversed."""
        ...
    
    def reset_to_first(self) -> None:
        """Resets the iterator to point to the first element"""
        ...

    def __len__(self) -> None:
        """
        Returns the total amount of resources in the iterator. This is only required if you use a progress bar and is used to compute the percentage of completed work
        """
        ...
```

A note on **multithreading**: If you intend to multithread your conversion with multiple workers (see chapter [Converter](#converter)), be aware that `iterator.next()` is not parallelised. If you want to leverage multiple threads for loading remote data, you must implement this in the [`Resource`](api.md#Resource) class (in `__getitem__`).

### IteratorIterator
The [`IteratorIterator`](api.md#iteratoriterator) allows you to iterate over multiple "sub"-iterators. There are no restrictions on the "sub"-iterators, as long as they are of type [`ResourceIterator`](api.md#resourceiterator). Since an [`IteratorIterator`](api.md#iteratoriterator) is also of type [`ResourceIterator`](api.md#resourceiterator), it can be used recursively.
```python
from rel2graph import IteratorIterator
iterator1 = ... # An iterator
iterator2 = ... # Another iterator
itit = IteratorIterator([iterator1, iterator2])
```
### Existing relational modules
#### Pandas
With the [`PandasDataFrameIterator`](api.md#pandasdataframeiterator) you can wrap a [pandas dataframe](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html?highlight=dataframe#pandas). If you pass a pandas dataframe to the [`PandasDataFrameIterator`](api.md#pandasdataframeiterator) it will automatically create [`PandasSeriesResource`](api.md#pandasseriesresource)s out of all rows (series) and iterate over them. Since a dataframe has no type associated, you need to also provide a type name.
```python
from rel2graph.relational_modules.pandas import PandasDataframeIterator
iterator = PandasDataframeIterator(pandas.Dataframe(...), "MyType")
```
#### OData
With the [`ODataListIterator`](api.md#odatalistiterator) you can iterate over a list of OData entities from [pyodata](https://pyodata.readthedocs.io/en/latest/). The entities need to be of type or behave like [*pyodata.v2.service.EntityProxy*](https://pyodata.readthedocs.io/en/latest/).
```python
from rel2graph.relational_modules.odata import ODataListIterator
iterator = ODataListIterator([entity1, entity2,...])
```

## Building your own Wrappers
A wrapper allows you to inject custom code into the conversion. A wrapper inserts preprocessing and postprocessing before and after a wrapped factory. There are three possibilities on how to create such factory wrappers. The simplest way is to define either a **preprocessor** (processing the [`Resource`](api.md#Resource) before it is passed to the factory) or a **postprocessor** (processing the factory's output). They are created by writing a simple python function. If you need more sophisticated functionality that uses both, you can define an entire wrapper class. This chapter will guide you through the creation of your own wrappers.

<img src="assets/images/wrapper.jpg" alt="drawing" width="400"/>

### Background
First, we need to be aware of the different factory types *rel2graph* uses. The input of every factory is always a resource, but depending on the type the output varies. To write wrappers, we need to distinguish the two main factory types: **SubgraphFactories** and **AttributeFactories**. 

<img src="assets/images/factory_hierarchy.png" alt="drawing" width="400"/>

#### AttributeFactories
*AttributeFactories* produce `Attribute` objects. An `Attribute` is a simple object that behaves as follows:
```python
from rel2graph import Attribute
myattr = Attribute("mykey", "myvalue")
key = myattr.key # get the key of the attribute
value = myattr.value # get the value of the attribute
```
An `Attribute` is immutable, so it can't be changed. If you want to change an existing attribute, you must create a new one with the key/value of the existing attribute including the change and return the new attribute.

Whenever you refer to an entity attribute, the parser in the [`Converter`](api.md#Converter) will create an *AttributeFactory* (for example `mykey = EntityName.attribute` or just `EntityName.attribute`). Note that a static attribute will also create an AttributeFactory that will just ignore the input resource (for example, `key = "staticstring"`). Given a resource the AttributeFactory that is created from `mykey = EntityName.attribute` will produce the attribute `Attribute("mykey", **value at resource.attribute**)`.

#### SubgraphFactories
*SubgraphFactories* produce, as the name suggests, a py2neo [Subgraph](https://py2neo.org/2021.1/data/index.html#subgraph-objects) object containing py2neo [Nodes](https://py2neo.org/2021.1/data/index.html#node-objects) and [Relationships](https://py2neo.org/2021.1/data/index.html#relationship-objects). When your write `NODE(...)` or `RELATION(...)` in the schema file, the parser will create a *NodeFactory* or a *RelationFactory*, respectively, out of your specification. Both of them are *SubgraphFactories*. The *NodeFactory* returns a subgraph with a maximum of one node, and the *RelationFactory* returns a subgraph with an arbitrary number of relationships. The nodes and relations of a subgraph can be accessed with `subgraph.nodes` and `subgraph.relationships`. Please check out the documentation of the py2neo objects for details about how to operate with them (click on the links).

### Registering
When we write a pre/postprocessor function or a wrapper class, we need to register it such that the [`Converter`](api.md#Converter) knows of its existence. Registering is done with python **decorators**. When registering pre/postprocessors we need to specify if it's for an *AttributeFactory* or a *SubGraphFactory*. A wrapper class needs no further specification. The following decorators are available for registering:
- `register_attribute_preprocessor`
- `register_attribute_postprocessor`
- `register_subgraph_preprocessor`
- `register_subgraph_postprocessor`
- `register_wrapper` 

The library will not check if your registered functions/classes match the expected format. If the function behaves other than expected, this will result in undefined behaviour during runtime. Make sure you define your functions/classes correctly.

### Preprocessors
A preprocessor transforms the resource before it reaches the factory. We write a function that takes a resource as input to define a preprocessor. If a factory gets `None` as input, it will simply create nothing. Therefore, if you want the factory only to produce an object if a condition is given, you can write a preprocessor and return `None` if the resource does not meet the requirement.

We can pass static (string) arguments from the schema file to a preprocessor. Simply add them in your function as parameters behind the resource and specify the arguments in the schema file.
Some examples:
```python
from rel2graph import register_attribute_preprocessor, register_subgraph_preprocessor

@register_attribute_preprocessor
def my_attr_preprocessor(resource: Resource) -> Resource:
    # do something to the resource
    ...
    return resource

@register_subgraph_preprocessor
def only_create_subgraph_if_preprocessor(resource: Resource, key: str, value="can also have a default value": str) -> Resource:
    """Only creates the subgraph if resource[key] == value"""
    if resource[key] != value:
        return None # do not create this subgraph
    return resource
```
`schema.yaml`
```yaml
ENTITY("type"):
    only_create_subgraph_if_preprocessor(NODE("label"), "somekey", "specificvalue"):
        - mykey = my_attr_preprocessor(type.myvalue)
```
The node "label" is only created if the attribute "somekey" of the "type" resource is exactly "specificvalue".

### Postprocessors
A postprocessor transforms the result of the factory. To define a postprocessor, we write a function that takes an attribute/subgraph as input, depending on the type. As described in [Preprocessors](#preprocessors), one can pass static (string) arguments to a postprocessor from the schema file.

Some examples:
```python
from rel2graph import register_attribute_postprocessor, register_subgraph_postprocessor, Attribute

@register_attribute_postprocessor
def attr_append_postprocessor(attribute: Attribute, value=" appendix": str) -> Attribute:
    """Append the value to the attribute"""
    new_attr = Attribute(attribute.key, attribute.value + value) # Attribute is immutable -> create new
    return new_attr

@register_subgraph_postprocessor
def my_subgraph_postprocessor(subgraph: Subgraph) -> Subgraph:
    # do something with the subgraph
    ...
    return subgraph
```
`schema.yaml`
```yaml
ENTITY("type"):
    my_subgraph_postprocessor(NODE(attr_append_postprocessor("label"))):
        - mykey = an_attr_preprocessor(attr_append_postprocessor(type.myvalue)) # you can mix pre and postprocessors
        - another = attr_append_postprocessor("static value", "i append this")
```
This will create a node with label "label appendix". The value of the attribute "another" is "static valuei append this".

### Full Wrappers
If you require more sophisticated functionality, like, for example, passing information from preprocessing to postprocessing or a state, you can create full wrapper classes. They need to inherit from either `SubgraphFactoryWrapper` or `AttributeFactoryWrapper`. Their constructor takes as the first parameter the wrapped factory, with which the parent's constructor is called. As for pre/postprocessor functions, the constructor can take static string arguments from the schema file. Further, the wrapper class needs to implement the `construct(resource)` method. To get the resulting product of the wrapped factory, call `super().construct(resource)` in your `construct` function.

The following example checks that at least one relation exists in the resulting subgraph, iff the provided resource is not None. This could not be done with simple pre/postprocessor functions. Obviously, everything that can be done with pre/postprocessor functions can also be done with full wrapper classes. 
```python
from rel2graph import SubGraphFactoryWrapper, register_wrapper

@register_wrapper
class REQUIRED(SubgraphFactoryWrapper):
    def __init__(factory: SubgraphFactory, static_string_parameter: str):
        super().__init__(factory)
        self.error_msg = static_string_parameter

    def construct(resource: Resource) -> Subgraph:
        subgraph = super().construct(resource)
        if resource is None:
            return subgraph # resource was None -> no check
        else:
            if len(subgraph.relationships) == 0:
                raise Exception(self.error_msg)
            return subgraph # condition is met -> return produced subgraph
```
`schema.yaml`
```yaml
ENTITY("type"):
    ...
    REQUIRED(RELATION(from, "relation type", MATCH("other", key="value")), "No match for label other and key=value"):
```
## Common modules
The rel2graph library comes with some predefined wrappers. To use any of them you must import them:
```
import rel2graph.common_modules
```
- **DATETIME**:
The DATETIME attribute wrapper allows to convert strings into datetime objects. It uses the datetime strptime function to convert the strings to datetime based on some formatting. The default formating string is `"%Y-%m-%dT%H:%M:%S"`. You can provide your own formating string as static argument in the conversion schema: `- datetime = DATETIME(entity.datetime_as_str, "%Y/%m/%d %H:%M:%S")`. If the provided argument is a datetime instead of a string, it will just remove any timezone information. Check the [datetime documentation](https://docs.python.org/3/library/datetime.html) for details about how strptime works.
- **DATE**:
The DATE attribute wrapper allows to convert strings into date objects. It uses the datetime strptime function to convert the strings to datetime based on some formatting and from there into just a date. The default formating string is `"%Y-%m-%d"`. You can provide your own formating string as static argument in the conversion schema: `- date = DATETIME(entity.date_as_str, "%Y/%m/%d  %H:%M:%S")`. Check the [datetime documentation](https://docs.python.org/3/library/datetime.html) for details about how strptime works. If the attribute passed to DATE contains also time information, this is simply stripped away (the format string still must fit the exact format of your attribute). If the provided argument is a datetime instead of a string, it will just remove any timezone information and convert it to date.

**Note**: If you encounter the exception `TypeError: Neo4j does not support JSON parameters of type datetime` for the DATE/DATETIME wrappers, make sure that you use the bolt/neo4j scheme with your graph. Dates won't work over *http*: 
```g = Graph(scheme="bolt", host="localhost", port=7687,  auth=('neo4j', 'password'))```


## Py2neo Extensions
The rel2graph library relies on py2neo for the connection to a neo4j graph. As py2neo is limited for specific use-cases, this library provides some helpful extensions.
### Graph with parallel relations
Py2neo does not allow the creation of parallel relations: Two parallel relations of the same type between the same two nodes. If two parallel relations are created, they are automatically merged.

<img src="assets/images/parallel_relations.png" width="300"/>

Use the `GraphWithParallelRelations` class to initiate your graph before passing it to the rel2graph [`Converter`](api.md#Converter) to allow for such parallel relations. The `GraphWithParallelRelations` behaves like a normal py2neo graph but supports the creation of parallel relations. If you want to merge relations with a `GraphWithParallelRelations` you can specify a [primary attribute for a relation](#merging-relations).
```python
from rel2graph.py2neo_extensions import GraphWithParallelRelations
from rel2graph import Converter ...
graph = GraphWithParallelRelations(scheme="http", host="localhost", port=7474,  auth=('neo4j', 'password'))
iterator = ...
converter = Converter(config_filename, iterator, graph)
converter()
```
Note that only `graph.create` and `graph.merge` are tested. No other functionality is guaranteed. It is suggested to use the normal `py2neo.Graph` class for interaction with a graph other than a conversion with rel2graph. Querying a neo4j graph with parallel relations should be no problem with the normal `py2neo.Graph` class. 

## Information for developers
If you intend to extend the library, please check out the [class diagram of the core of the library](assets/pdfs/class_diagram_core.pdf).
### Testing
The library uses [pytest](https://docs.pytest.org) for testing. Please implement tests for your code. Use the following snipped to run the tests.
```bash
$ python -m pytest
```
Any new tests are to be put in the *tests/{unit,integration}* folder respectively.
