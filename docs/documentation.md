# Documentation
**DOCUMENTATION STILL UNDER CONSTRUCTION**

This is the documentation for the *rel2graph* library. For developers intending to work on the library itself, please refer to [here](#information-for-developers).

For a Quick Start Example please refer to the [ReadMe](../README.md).

## Contents

- [Quick Start Example](../README.md)
- [Introduction](#introduction) 
- [Converter](#converter)
    - [Statefullness](#statefullness)
    - [Logging and progress monitoring](#logging-and-progress-monitoring)
- [Schema Syntax](#schema-syntax)
- [Customizing Resource and ResourceIterators](#customizing-resource-and-resourceiterators)
    - [Existing relational modules](#existing-relational-modules)
        - [Pandas](#pandas)
        - [OData](#odata)
- [Building your own Wrappers](#building-your-own-wrappers)
- [Information for developers](#information-for-developers)
    - [Testing](#testing)
</br>
</br>

**Rel2graph** is a library that simplifies the convertion of data in relational format to a graph knowledge database. It reliefs you of the cumbersome manual work of writing the conversion code and let's you focus on the conversion schema and data processing.

The library is built specifically for converting data into a [neo4j](https://neo4j.com/) graph. The library further supports extensive customization capabilities to clean and remodel data. As neo4j python client it uses the [py2neo](https://py2neo.org/2021.1/index.html) library.

## Introduction
This chapter will give you an overview how *rel2graph* works and give you a first intuition on how to interact with it. Details on how the individual parts of the library work can be found in later chapters. Simplified, the library works like a factory that converts an input, some relational data, into an output, a [neo4j](https://neo4j.com/) graph. The factory input is called a [`Resource`](api.md#Resource). A [`Resource`](api.md#Resource) can wrap any relational entity. For every supplied resource the factory will produce a graph. We define a **conversion schema** ahead that specifies the "factory blueprints": what it produces and how. Once the factory is setup and knows the schema, we can keep supplying it with resources without needing to write more code. 

<img src="assets/images/factory.png" alt="drawing" width="800"/>

Since there might be different types of resources we build a factory per resource type. One specifies all the "blueprints" for all the factories in a **conversion schema** file. A [`Converter`](api.md#Converter), the main object of *rel2graph*, will take this file and construct all the factories based on your "blueprints". For a set of supplied resource the [`Converter`](api.md#Converter) will automatically select the correct factory, use it to produce a graph out of the resource and merge the produced graph with the full [neo4j](https://neo4j.com/) graph. We supply resources to the converter with a [`ResourceIterator`](api.md#ResourceIterator). This iterator keeps track of what the next resource to process is. The [`Resource`](api.md#Resource) and [`ResourceIterator`](api.md#ResourceIterator) classes can be fully customized. A simple version of it might just point to a specific element in a list of resources, as visualized in the image below. The [`Converter`](api.md#Converter) iteratively asks the [`ResourceIterator`](api.md#ResourceIterator) for the next resource until the iterator reports no more resources to process.

<img src="assets/images/overview.png" alt="drawing" width="800"/>

At the simplest the library consists of the following 4 parts: 
- [`Converter`](api.md#Converter): handles all the factories and building the graph.
- The **conversion schema**, specifying what is converted into what. 
- [`Resource`](api.md#Resource): A wrapped relational entity 
- [`ResourceIterator`](api.md#ResourceIterator): An iterator for the to-be-processed resources.

The next chapters will go into detail about these 4 parts. In later chapters we will show you how can insert your custom code into one of these factories by creating [Wrappers](#Wrappers). Wrappers can apply a pre- and/or postprocessing to a factory. 
<img src="assets/images/wrapper.jpg" alt="drawing" width="600"/>
If you wrap for example a node factory, a wrapper first preprocesses the resource, then creates the node with the wrapped factory and lastly postprocess the created node itself. A wrapper behaves itself like a factory and can be wrapped into another wrapper itself. This allows you to insert arbitrary customization into the conversion and adapt it to your usecase.
## Converter
The [`Converter`](api.md#Converter) handles the main conversion of the relational data. It is initialised with the *conversion schema filename*, the iterator and the graph. 
```python
from rel2graph import Converter

converter = Converter(config_filename, iterator, graph)
```
To start the conversion, one simply calls the object. It then iterates twice over the iterator: first to processes all the nodes and then, secondly, to create all relations. This makes sure that any node a relation refers to, is already created first.
```python
converter()
```
The [`Converter`](api.md#Converter) can utilise **multithreading**. When initializing you can set the number of parallel workers. Each worker operates in its own thread. Be aware that the commiting to the graph is often still serialized, since the semantics require this (e.g. nodes must be commited before any relation or when [mergin nodes](#merging-nodes) all the nodes must be serialy commited). So the primary usecase of using multiple workers is if your resources are utilizing a network connection (e.g. remote database) or if you require a lot of [matching](#match) in the graph (matching is parallelised).
```python
converter = Converter(config_filename, iterator, graph, num_workers = 20)
```
**Attention:** If you enable more than 1 workers, make sure that all your [custom wrappers] support multithreading (add locks if necessary).

### Statefullness   
The converter is **stateful**. If you call the converter in a *jupyter notebook* and an exception is raised during the execution of the converter (e.g. due to an KeyboardInterrupt, a network problem, an error in the conversion schema file), you can fix the problem and call the converter again. The converter will the continue its work, where it left of (the resource that was responsible for the exception, will be reconverted). **It is guaranteed that no resource is lost.**

The state of the converter is reset when *initializing it* or when *you change the underlying iterator* with `converter.iterator = new_iterator`. So if you don't want the stateful behaviour you need to do one of these two things.
#### Example 1
You use OData Resources and the network connection suddenly drops out.
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
You have a small error in your **conversion schema** for a specific entity (e.g. a typo in an attribute key), which is not immediatelly a problem since you first process a lot other entities. Again in the first cell, you initially have created the converter object and called it.
```python
converter = Converter(config_filename, iterator, graph)
converter()
```
Now e.g. `KeyError `is raised since the attribute name was written slighly wrong. Instead of rerunning the whole conversion (which might take hours) you can fix the schema file and reload the schema file and recall the converter:
```python
converter.reload_config(config_filename)
converter()
```
The converter will just continue where it left off with the new *conversion schema*. 

### Logging and progress monitoring
The whole rel2graph library uses the standart python [logging](https://docs.python.org/3/howto/logging.html) library. See an example on how to use it below. For more information check out the [official documentation](https://docs.python.org/3/howto/logging.html).
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

The **conversion schema** defines, which relational entities are converted to which graph elements (nodes and relations). As we have seen in the [Introduction](introduction.md), the [`Converter`](api.md#Converter) expects resources as inputs. A resource is a wrapper around a relational entity. Each resource has a *type* associated with it that corresponds to the *type* of entity it wraps. The *type* must be defined for every [`Resource`](api.md#Resource) instance and accessible at `Resource.type`. In the schema file we speficy for each *type* what the [`Converter`](api.md#Converter) should do when it encounters this *type*. The schema allows for one-to-one, one-to-many and many-to-one (with the help of [merging](#merging-nodes)) conversions of relational entities to graph elements (nodes and relations).

We define this behavior in the schema file. The file follows a modified YAML schema syntax. We will now look at our example from the [Quick Start](../README.md). 
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
A node is defined with `NODE(`*label1, label2, ...*`)`, where in between the brackets we define the labels that the nodes should have. We can also define the label based on an attribute of the entity: `NODE(`*type.attributename*`)` (e.g. `NODE("Flower", Flower.species)` to add the species name as label to the node). After a node definition we can specify its internal **identifier**. The identifier is optional and is used for refering to this node, when creating relations as we will see later. The **identifier** is only valid within one entity and therefore must be unique per entity. The full syntax for defining an node is:
`NODE(`*`label1, label2, ...`*`) `**`identifier`**`:`

### Attributes
Under a node or a relation we can define its attributes as follows (indented following YAML format):
`-`*`attribute_name`*`=`*`type.entity_attribute_name`*
Going back to our example, if the node with identifier **flower** should have an attribute named `sepal_length` that contains the value of the attribute `sepal_length` of the entity "Flower", we write 
`- sepal_length  = Flower.sepal_length`.
The attribute name of the node/relation must not be the same as the one of the entity. We could also do 
`- sl  = Flower.sepal_length` 
to get a node with attribute `sl`.

### Relation
A relation is declared with `RELATION(`*source node(s)*`,`*relation type*`,`*destination node(s)*`)`. The relation type is a simple string, that represents the name of the relation. This will create a relation on the kartesian product of the *source node(s)* and the *destination node(s)* (from all sources to all destinations). We have two options on how to set source and destination node: 
- Use a node identifier (note that it must appear above the relation declaration under the same entity). This allows us to set a single node. E.g. in our example we have defined the two node with identifiers `flower` and `species`. We can now define a relation between those too with `RELATION(flower, "is", species)`.
- Use the `MATCH` keyword. With a matcher we can query for arbitrary nodes in the graph. This is useful, when the node we want to refer to is either from a different instance of the same entity or from a different entity (or already existing in the graph). A matcher can return a single or multiple nodes. 

### Match
The match syntax works as follows: `MATCH(`label1`,` label2`,` ...`,`attribute1=value1`,`attribute2=value2`,`...`)`. We first specify the required labels of the searched node(s). We can define arbitrary many labels and the labels themselves can be extracted from the entity as we did with the nodes (e.g. `Flower.species`). We then define the conditions that the nodes we are looking for must meet by specifying which attributes must have which values. We write *myattribute=myvalue*, meaning that any matched node must have the value *myvalue* for its attribute *myattribute*. Again the value can be extracted from the entity (e.g. `Name=Person.FavoriteFlower`). We can specify an arbitrary amount of conditions. 


### Merging Nodes
If we expect that a node is created multiple times and we want to ensure that it is only created once, we can specify a primary attribute. In our example, this would be the case for the node with identifier **species**. If multiple rows of the "Flower" table contain the same entry for the "species" column, the converter will create a node for this species for each row. So for our example table we would end up with at least two "setosa" nodes. What we want, however, is only one node for each species present. 

For this purpose, we can specify a **primary attribute** and a **primary label** to merge a node with the graph. The **primary label** is always the first one mentioned, so we reformulate the node definition to `NODE(`**`primary_label`***`, label2, ...`*`)`. The **primary attribute** is set by replacing the `-` in the attribute definition with a `+`:
`+`*`attribute_name`*`=`*`type.entity_attribute_name`*
If the `Converter` detects that a primary attribute* is set, it will only create a new node if **no** node with the same primary label and primary attribute exists in the graph. If the node already exists, it is updated, i.e. new attributes are added and existing attributes are updated according to the specified values. 

We can also use this to create nodes with informations from different entity types. For example lets assume we had an entity "Person" and an entity "Employee", both of them containing a per-person-unique property:

Table "Person"
| personId | name | ... |
|--------------|-------------|---------|
| ...          | ...         | ...     |


Table "Employee"
| personId | employer | ... |
|--------------|-------------|---------|
| ...          | ...         | ...     |

To create a node that contains both attributes of the entity "Person" and the entity "Employee" we can use the above explained syntax to merge the nodes:
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
If you now supply for every person both entities to the converter the resulting nodes will have all the attributes `id`, `name` and `employer`. Note if you don't supply both entities for a person the node will only contain the information from the single entity that it got.


## Customizing Resource and ResourceIterators
coming soon
### Existing relational modules
coming soon
#### Pandas
coming soon
#### OData
coming soon

## Building your own Wrappers
coming soon
## Information for developers
If you intend to extend the library, please check out the [class diagram of the core of the library](assets/pdfs/class_diagram_core.pdf).
### Testing
The library uses [pytest](https://docs.pytest.org) for testing. Please implement tests for your code. Use the following snipped to run the tests.
```bash
$ python -m pytest
```
Any new tests are to be put in the *tests/{unit,integration}* folder respectively.
