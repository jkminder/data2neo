# Developer Interface
This is the part of the documentation that covers all the main interfaces of the *rel2graph* library. Any abstract method or class is marked with 🟨.

* **[Main Interface](#main-interface)**
    * [Converter](#converter)
    * [IteratorIterator](#iteratoriterator)
    * **[Abstract Classes](#abstract-classes)**
        * [Resource](#resource)
        * [ResourceIterator](#resourceiterator)
* **[Relational Modules](#relational-modules)**
    * **[OData](#odata)**
        * [ODataResource](#odataresource)
        * [ODataListIterator](#odatalistiterator)
    * **[Pandas](#pandas)**
        * [PandasSeriesResource](#pandasseriesresource)
        * [PandasDataframeIterator](#pandasdataframeiterator)




## Main Interface
The main api and classes provided by the library.

----
#### Converter
`class rel2graph.`**`Converter`**`(config_filename, iterator, graph, num_workers = 20)` [[source]](https://github.com/sg-dev/rel2graph/blob/main/rel2graph/core/converter.py)

The main converter object handling all the conversion. The converter keeps its progress state, unless the iterator is changed. When interrupted it can continue working where it has left off.

**Parameters**:
* **config_filename**: *str*\
Path to conversion schema configuration file. Must follow the schema syntax.
* **iterator**: *[ResourceIterator](#resourceiterator)*\
Iterator of the data that should be converted.
* **iterator**: *[py2neo.Graph](https://py2neo.org/2021.1/workflow.html#py2neo.Graph)*\
The *py2neo neo4j* graph where the converter should commit to.
* **num_workers**: *int*\
Number of parallel threads working. Note that only interaction with the resource is parallelized, so if your data is primarily local, this might not give a performance boost. Further make sure your wrappers support parallelizing.

**Attributes**:
* **iterator**: Gets and sets the *[ResourceIterator]()*.

**Methods**:
* **\_\_call\_\_(progress_bar = None, skip_nodes = False, skip_relations = False)**:\
    Starts the converter.
    **Parameters**:
    * **progress_bar**: [tqdm.tqdm](https://tqdm.github.io/)\
    A custom uninizialised progress bar (link to object). If None, no progress bar is displayed.
    * *skip_nodes*: bool\
    EXPERIMENTAL: Will skip processing all nodes. WARNING: Only recommended if you know the library well. Will produce problems with identifiers.
    * *skip_relations*: bool\
    EXPERIMENTAL: Will skip processing all relations. WARNING: Only recommended if you know the library well.
* **reload_config(config_filename)**:\
    Reload the conversion schema from disk from the config file.
    **Parameters**:
    * **config_filename**: str\
    Path to conversion schema configuration file. Must follow the schema syntax.


**Usage**
```python
# Construct with 1 worker
converter = rel2graph.Converter("myschema.yaml", resource_iterator, my_graph, num_workers = 1)

from tqdm import tqdm 
converter(progress_bar = tqdm) # Start conversion with tqdm progress bar
```

----
#### IteratorIterator
`class rel2graph.`**`IteratorIterator`**`(iterators)` [[source]](https://github.com/sg-dev/rel2graph/blob/main/rel2graph/core/resource_iterator.py)

Implements [ResourceIterator](#resourceiterator). Iterates over a list of iterators.

**Methods**:
* **next( )**:\
    Gets the next resource that should be converted. If an iterator is traversed, it will return the first element of the next iterator in the list of iterators. Returns None if all the iterators are traversed.\
    **Return type:** [Resource](#resource)\
* **reset_to_first( )**:\
    Resets the iterator to point to the first element of the range (of the first iterator).
* **\_\_len__( )** :\
    Retrieves the sum of all lengths of all iterators.\
    **Return type:** int
    
**Usage**
```python
iterator = rel2graph.IteratorIterator([myfirstiterator, anotheriterator])
```


<br />

### Abstract Classes

----
#### Resource

🟨 `class rel2graph.`**`Resource`**`()` [[source]](https://github.com/sg-dev/rel2graph/blob/main/rel2graph/core/factories/resource.py)

Wraps an relational entity and is used as input data object for the converter. May hold additional supplies to pass data between factories.

**Attributes**:
* **supplies**: Dict[str, Any]\
A dictionary. Used to pass data between factories. Must not be customized.
* 🟨 **type**: str\
The type/name of the resource as a string. Used to determine the correct factory. Must be defined by inheriting classes.

**Methods**:
* 🟨 **\_\_repr__( )** :\
    A string representation of the resource. Used for logging purposes.\
    Should follow the format:\
    ```NameOfResource 'TypeOfResource' (DetailsAboutResource)```\
    Example-Implementation:\
    `return f"{super().__repr__()} ({self.somedetail})"`\
    **Return type:** str
* 🟨 **\_\_getitem__(key)** :\
    Retrieves a value with key key from the underlying relational entity.
    **Parameters:**
    * **key**: str\
    The key that should be retrieved from the underlying entity.
    **Return type:** str
* 🟨 **\_\_setitem__(key, value)** :\
    Sets a value with key key from the underlying relational entity.\
    **Parameters**
    * **key**: str\
    The key that should be set from the underlying entity.
    * **value**: str\
    The value that should be set in the underlying entity.\
 
* **clear_supplies**( )\
    Clears the supplies.
    
For an example of an implementation of a resource check out [ODataResource](#odataresource).

----
#### ResourceIterator

🟨 `class rel2graph.`**`ResourceIterator`**`()` [[source]](https://github.com/sg-dev/rel2graph/blob/main/rel2graph/core/resource_iterator.py)

Iterates over a range of resources. Since the converter needs to run over the data twice (once for nodes and once for relations) the iterator needs to be resetable to the first element.

**Methods**:
* 🟨 **next( )** :\
    Gets the next resource that should be converted. Returns None if the range is traversed.\
    **Return type:** [Resource](#resource)
* 🟨 **reset_to_first( )** :\
    Resets the iterator to point to the first element of the range.
* 🟨 **\_\_len__( )** :\
    Retrieves the length of the underlying range. Is ONLY used for progress reporting. Can be approximate.\
    **Return type:** int
    
For an example of an implementation of a resource check out [ODataResourceIterator](#odataresourceiterator).

## Relational Modules

The provided implementations for relational entities. They implement the [abstract classes](#abstract-classes) of the main interface. 

</br>

### OData
Implements the [Resource](#resource) and [ResourceIterator](#resourceiterator) for OData based on [py2neo](https://py2neo.org/2021.1/index.html).

----
#### ODataResource
`class rel2graph.relational_modules.odata.`**`ODataResource`**`(entity)` [[source]](https://github.com/sg-dev/rel2graph/blob/main/rel2graph/relational_modules/odata.py)

Wraps an odata entity and is used as input data object for the converter. May hold additional supplies to pass data between factories.

**Parameters**:
* **entity**: [*pyodata.v2.service.EntityProxy*](https://pyodata.readthedocs.io/en/latest/)\
Wrapped OData entity.

**Attributes**:
* **supplies**: Dict[str, Any]\
A dictionary. Used to pass data between factories.
* **type**: str\
Returns the type/name of the underlying odata entity as a string (pyodata.v2.service.EntityProxy.entity_set.name). Used to determine the correct factory.
* **odata_entity**: [*pyodata.v2.service.EntityProxy*](https://pyodata.readthedocs.io/en/latest/)\
Gets the wrapped odata entity.

**Methods**:
* **\_\_repr__( )**:\
    A string representation of the resource. Used for logging purposes. Returns `ODataResource '{type}' {entity}`. Where type is the resource type and entity the string represenation of the odata entity.\
    **Return type:** str
* **\_\_getitem__(key)**:\
    Retrieves a value with *key* key from the underlying odata entity.\
    **Parameters**:
    * **key**: str\
    The key that should be retrieved from the odata entity.\
    **Return type:** str
* **\_\_setitem__(key, value)**:\
    Sets a value with key key from the underlying odata entity.\
    **Parameters**:
    * **key**: str\
    The key that should be set from the odata entity.
    * **value**: str\
    The value that should be set in the odata entity.\
* **clear_supplies**( )\
    Clears the supplies.
    
----
#### ODataListIterator
`class rel2graph.relational_modules.odata.`**`ODataListIterator`**`(entities)` [[source]](https://github.com/sg-dev/rel2graph/blob/main/rel2graph/relational_modules/odata.py)

Implements [ResourceIterator](#resourceiterator). Iterates over a list of odata entities (pyodata). Automatically creates the [ODataResource](#odataresource)s from the list of odata entities.

**Parameters**:
* **entities**: List[ [*pyodata.v2.service.EntityProxy*](https://pyodata.readthedocs.io/en/latest/) ]\
List of OData entities.\

**Methods**:
* **next( )**:\
    Gets the next resource that should be converted. Returns None if the list is traversed.\
    **Return type:** [ODataResource](#odataresource)
* **reset_to_first( )**:\
    Resets the iterator to point to the first element of the range.\
* **\_\_len__( )** :\
    Retrieves the length of the list of odata entities.\
    **Return type:** int

</br>

### Pandas

Implements the [Resource](#resource) and [ResourceIterator](#resourceiterator) for [pandas](https://pandas.pydata.org) dataframes.

----
#### PandasSeriesResource

`class rel2graph.relational_modules.pandas.`**`PandasSeriesResource`**`(series, type)` [[source]](https://github.com/sg-dev/rel2graph/blob/main/rel2graph/relational_modules/pandas.py)

Wraps a pandas series and is used as input data object for the converter. May hold additional supplies to pass data between factories.

**Parameters**:
* **series**: [*pandas.Series*](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html?highlight=series#pandas.Series)\
Wrapped pandas series.
* **type**: str\
Name of type that this series is an entity of.

**Attributes**:
* **supplies**: Dict[str, Any]\
A dictionary. Used to pass data between factories.
* **type**: str\
Returns the type/name of the underlying odata entity as a string (pyodata.v2.service.EntityProxy.entity_set.name). Used to determine the correct factory.
* **series**: [*pandas.Series*](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html?highlight=series#pandas.Series)\
Gets the wrapped pandas series.

**Methods**:
* **\_\_repr__( )**:\
    A string representation of the resource. Used for logging purposes. Returns `PandasSeriesResource '{type}' (row {series.name})`. Where type is the resource type and series.name the name of the series, which is, if not specifically changed, the row in the dataframe.\
    **Return type:** str
* **\_\_getitem__(key)**:\
    Retrieves a value from *column* key from the pandas series.\
    **Return type:** str
    **Parameters**:
    * **key**: str\
    The column that should be retrieved from the pandas series.\
* **\_\_setitem__(key, value)**:\
    Sets a value with column key from the pandas series.\
    **Parameters**:
    * **key**: str\
    The *column* key that should be set from the pandas series.
    * **value**: str\
    The value that should be set in the pandas series.
* **clear_supplies**( )\
    Clears the supplies.
    
----
#### PandasDataframeIterator

`class rel2graph.relational_modules.pandas.`**`PandasDataframeIterator`**`(dataframe, type)` [[source]](https://github.com/sg-dev/rel2graph/blob/main/rel2graph/relational_modules/pandas.py)

Implements [ResourceIterator](#resourceiterator). Iterates over the rows (pandas series) in a pandas dataframe. Automatically creates the [PandasSeriesResource](#pandasseriesresource)s from the dataframe.

**Parameters**:
* **dataframe**: *[pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html?highlight=dataframe#pandas.DataFrame)*
* **type**: str\
Name of type of data that the dataframe contains entities of.

**Methods**:
* **next( )**:\
    Gets the next resource that should be converted. Returns None if the dataframe is traversed.\
    **Return type:** [PandasSeriesResource](#pandasseriesresource)
* **reset_to_first( )**:\
    Resets the iterator to point to the first element of the range.
* **\_\_len__( )** :\
    Retrieves the length of the dataframe (amount of rows).\
    **Return type:** int
    
