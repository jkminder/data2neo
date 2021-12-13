# rel2graph
# Deprecated ReadMe, will soon be updated

## Project Description
Converts a relational database to a graph knowledge database. More precisiely it converts a [OData](https://www.odata.org/) database into a [neo4j](https://neo4j.com/) database and adds extensive capabilities to clean and remodel data. As neo4j python client it will use the [py2neo](https://py2neo.org/2021.1/index.html) library.

## Requirements

The framework should be able to dynamically handle the following requirements. The checkbox signifies that the current design idea supports this already:

 - [x] Convert and clean individual attributes (e.g. Classify a free-text segment into classes)
 - [ ] Merge two or more entities into one node
 - [x] Create two or more connected nodes from one entity
 - [x] Create a relation from a foreign key
 - [x] Customize/Extend foreign keys
 - [x] Create a relation from an entity


## Draft for config layout and functionality
The config files follow the YAML style. The following is a initial draft for how the config files will look like and work. This may change significantly. It is primarily here to help me think about how the framework needs to be designed and what is required from it.


### One Entity to One Node
A node is specified by `NODE(label1, label2, ...) identifier`, followed by its attributes as a list. The `identifier` is only used in the config file to reference to this node (e.g. to specify a Relation from it). Therefore the `identifier` musst be unique in a config file. The identifier is optional and must not be specified if not needed.

`AttributeProcessor` is a abstract implementation of an operation on a attribute. An `AttributeProcessor` may take additional parameters. One example might be the `RegexProcessor` take applies some Regex cleaning to an attribute. The `AttributeProcessor` implementations are customly built by the users of the framework and not in the framework itself.

```yaml
ENTITY(entityname):
    NODE(primarylabel, secondarylabel) node:
        - attr1 = entityname.attr2
        - attr2 = SomeAttributeProcessor(entityname.attr3)
        - attr3 = RegexProcessor(entityname.attr5, "([A-Z])\w+")
```
### One Entity to Multiple Nodes with Relations

```yaml
ENTITY(entityname):
    NODE(primarylabel, secondarylabel) node1:
        - attr1 = entityname.attr2
    NODE(primarylabel, entityname.attr3) node2:
        - attr1 = entityname.attr4
```
### Relations and Matching
A relation is created with with the `RELATION` tag. It follows the syntax `RELATION(fromnode, label, tonodes)`. 
With `MATCH(labels, matchingproperties...)` one can specify existing nodes in the graph to link to. Note that all labels and properties are connected with AND.  

<span style="color:red">Implementation Note:</span> Match Relations musst be executed at the very end of creating all nodes, since otherwise it might be that some of the created 

```yaml
ENTITY(entityname):
    NODE(primarylabel, secondarylabel) node1:
        - attr1 = entityname.attr2
    NODE(primarylabel, entityname.attr3) node2:
        - attr1 = entityname.attr4
    RELATION(node1, label, node2): #one to one
        - attr = entityname.attr7
    RELATION(node2, label, MATCH([label1, label2], someproperty=somevalue, someotherproperty=node1.attr1)): #one to many with no attributes
```

### Relations and Foreign Keys
<span style="color:red">Implementation Note:</span> Will document more about how this works internally and how the library provides wrappers and how one can write custom wrappers.

If we want to match based on e.g. the id attribute of a foreign key table (nav property in OData), we write:
```yaml
ENTITY(entityname):
    NODE(primarylabel, secondarylabel) node1:
        - attr1 = entityname.attr2
    RELATION(node1, label, MATCH(label1, id = entityname.navproperty1->id): 
```
Upon loading the configuration parser basically converts this to
```yaml
ENTITY(entityname) entity:
    NODE(primarylabel, secondarylabel) node1:
        - attr1 = ATTRIBUTE(entityname.attr2)
    RELATION(node1, label, MATCH(label1, id = ForeignKeyAttribute(Attribute(entity, navproperty1),  "id")): 
```
Which for odata is implemented as:
```python
from AttributeWrappers import AttributeFactoryWrapper, register

@register
class Wrapper(AttributeFactoryWrapper):
    def preprocessor(resource):
        foreign_property = self.args[0]
        odata_entity = resource.entity.get_host_entity_object()
        foreign_entities = odata_entity.nav(self.entity_attribute_name).get_entities().filter(foreign_property).execute()
        resource.entity.update_attribute(nav_property, [getattr(foreign, foreign_property) for foreign in foreign_entities])
        return resource
```

As an example for a custom made attribute wrapper.
```yaml
ENTITY(business) entity:
    NODE(primarylabel, secondarylabel) node1:
        - attr1 = entityname.attr2
    RELATION(node1, label, MATCH(label1, id = get_transcript_ids(entity.id)): 
```

```python
from AttributeWrappers import register
from RelationalDatabase import Handler


@register
def get_transcript_ids(business_id_attribute):
    client = Handler.client
    subject_businesses = client.entity_sets.SubjectBusiness.get_entities()
    subject_businesses = subject_businesses.filter(f"BusinessNumber eq {business_id_attribute.value}").select("IdSubject").execute()
    sids = [sb.IdSubject for sb in subject_businesses] 
    for sid in subject_ids:
        transcripts[sid] = client.entity_sets.Transcript.get_entities().filter(IdSubject=sid, Language="DE").execute()
    return Attribute("Transcripts", transcripts)

```

### Conditions
With the `CONDITION(object, condition)` we can specify that the object should only be created if the condition is true. The condition musst be valid python code and result to a boolean value. 

In this example we create either a **node1** or a **node2** based on the `entityname.type`. 

```yaml
ENTITY(entityname):
    CONDITION(NODE(somelabel), entityname.type == 0)) node1:
        - attr1 = entityname.attr2
    CONDITION(NODE(anotherlabel), entityname.type == 1)) node2:
        - attr1 = entityname.attr4
```

## Implementation Notes and Caviats

 - <span style="color:red"> All relations and matching musst be done after creating all nodes. Pipeline musst therefore prioritise Node creation</span>

### Todo
 - Class Diagram and System Design
