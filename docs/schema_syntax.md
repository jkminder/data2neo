| [⭅ </br> Introduction](introduction.md) | [⭆ </br>Customizing Resource and ResourceIterators](resourcecustomization.md) |
|----------|----------|
# Schema Syntax
The *conversion schema* defines, which relational entities are converted to which graph elements (nodes and relations). As we have seen in the [Introduction](introduction.md), the `Converter` expects resources as inputs. A resource is a wrapper around a relational entity. Each resource has a *type* associated with it that corresponds to the *type* of entity it wraps. The *type* must be defined for every `Resource` instance and accessible at `Resource.type`. In the schema file we speficy for each *type* what the `Converter` should do when it encounters this *type*. The schema allows for one-to-one, one-to-many and many-to-one (with the help of [merging](#Merging\ Nodes)) conversions of relational entities to graph elements (nodes and relations).

We define this behavior in the schema file. The file follows a modified schema syntax. We will now look at our example from the [Quick Start](../README.md). 
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
Under an node or a relation we can define its attributes as follows (indented following YAML format):
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

| [⭅ </br> Introduction](introduction.md) | [⭆ </br>Customizing Resource and ResourceIterators](resourcecustomization.md) |
|----------|----------|
