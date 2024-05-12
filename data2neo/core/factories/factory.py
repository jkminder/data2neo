#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Factories for creating GraphElements from Resources

For the full inheritance structure please check out the class diagram in ./docs
authors: Julian Minder
"""

from abc import ABC, abstractmethod
from typing import List
import logging

from .resource import Resource
from .matcher import Matcher
from ...neo4j.graph_elements import Attribute, GraphElement, Node, Relationship, Subgraph
from .registrar import register_factory

logger = logging.getLogger(__name__)

class Factory(ABC):
    """Abstract factory for creating GraphElements from a Resource
    
    Attributes:
        id: A string that identifies this factory instantiation, usually a describing name
    """
    
    @abstractmethod
    def __init__(self, identifier: str = None) -> None:
        """Inits Factory with an identifier.
        
        Args:
            identifier: A string identifying this Factory instance. Can be None if factory doesn't need to save supplies.
                (supplies are saved in the resource for later consumption by other factories)
        """
        super().__init__()

        # Check if identifier exists
        if identifier is not None:
            self._id = identifier
        else:
            self._id = None
    
    @property
    def id(self) -> str:
        """A string that uniquely identifies this factory instantiation"""
        return self._id
    
    @abstractmethod
    def construct(self, resource: Resource) -> GraphElement:
        """Abstract function for constructing a GraphElement from a Resource.

        Args:
            resource: A Resource containing any information needed for the construction.
        Returns:
            A GraphElement (Subgraph or Attribute)
        """
        pass



class SubgraphFactory(Factory):
    """Abstract factory for creating Subgraphs from a Resource.
        
    Attributes:
        identifier: A string identifying this Factory instance.
    """

    @abstractmethod
    def construct(self, resource: Resource) -> Subgraph:
        """Abstract function for constructing a GraphElement from a Resource.

        If the resource is None, then this method will return an empty Subgraph.

        Args:
            resource: A Resource containing any information needed for the construction
        Returns:
            A GraphElement (Subgraph or Attribute)
        """
        pass


@register_factory
class AttributeFactory(Factory):
    """Factory that dynamically creates an Attribute given a Resource.
    
    If one sets the static_attribute_value to None, any attribute produced by this factory will 
    have resource.entity.entity_attribute as its value. If static_attribute_value is set to any string
    the factory will produce an Attribute(attribute_key, static_attribute_value) for all resources (inputs).

    Attributes:
        attribute_key: The key that any produced Attribute has.
        static_attribute_value: If this is set, then any attribute produced by this factory will have this string as its value
        entity_attribute: A key of an attribute of the expected resource entity.
    """

    def __init__(self, attribute_key: str, entity_attribute: str, static_attribute_value: str = None, identifier: str = None) -> None:
        """Inits an AttributeFactory with the following arguments.
        
        Args:
            attribute_key: The key that any produced Attribute should have.
            entity_attribute: The key of the attribute that is extracted from the resource entity.
            static_attribute_value: (default: None) If this is set, then any produced attribute will have this static value.
            identifier: A string identifying this Factory instance. Can be None if factory doesn't need to save supplies.
        """
        super().__init__(identifier)
        self._attribute_key = attribute_key
        self._entity_attribute = entity_attribute
        self._static_attribute_value = static_attribute_value

    @property
    def attribute_key(self) -> str:
        """The key that any produced Attribute has."""
        return self._attribute_key
    
    @property
    def entity_attribute(self) -> str:
        """A key of an attribute of the expected resource entity."""
        return self._entity_attribute

    @property
    def static_attribute_value(self) -> str:
        """Static value for any produced attribute"""
        return self._static_attribute_value

    def construct(self, resource: Resource) -> Attribute:
        """Constructs an Attribute from a resource based on the factory settings (refer to the class description).

        If the resource is None, then this method will return an None as well.

        Args:
            resource: A Resource containing any information needed for the construction
        Returns:
            An Attribute
        Raises:
            ValueError: Raised if the entity in the resource does not have the entity_attribute attribute.
        """
        if resource is None:
            return None

        if self.static_attribute_value is not None:
            return Attribute(self.attribute_key, self.static_attribute_value)
        else:
            # try to extract an attribute from the resource entity
            try:
                value = resource[self.entity_attribute]
            except ValueError:
                raise ValueError(f"AttributeFactory: Error while extracting the attribute {self.entity_attribute} from an entity with type {resource.entity.type}")
            return Attribute(self.attribute_key, value)


@register_factory
class NodeFactory(SubgraphFactory):
    """Factory for creating Nodes from a Resource"""

    def __init__(self, attributes: List[AttributeFactory], labels: List[AttributeFactory], primary_key: str = None, identifier: str = None) -> None:
        """Inits an NodeFactory with the following arguments
        
        Args:
            attributes: List of AttributeFactories for constructing the attributes for the node
            labels: List of AttributeFactories for constructing the labels for the node
            primary_key: Optional key of the primary attribute. Used to merge the produced node with existing nodes in the graph (default: None)
            identifier: A string identifying this Factory instance. Can be None if factory doesn't need to save supplies
        """
        super().__init__(identifier)
        self._attributes = attributes
        self._labels = labels
        self._primary_key = primary_key

    def construct(self, resource: Resource) -> Node:
        """Constructs an Node from a resource based on the the label and attribute factories provided
        when the instance was initialised.
        
        If the resource is None, then this method will return an empty Subgraph

        Args:
            resource: A Resource containing any information needed for the construction
        Returns:
            An Node
        """

        if resource is None:
            return Subgraph()
        labels = [label_factory.construct(resource) for label_factory in self._labels]
        attributes = [attr_factory.construct(resource) for attr_factory in self._attributes]
        return Node.from_attributes([l for l in labels if l is not None], [attr for attr in attributes if attr is not None], self._primary_key)

@register_factory
class RelationshipFactory(SubgraphFactory):
    """Factory for creating Relations from a Resource
    
    The RelationFactory is initialised with two Matcher objects (from_matcher and to_matcher) that specify how to
    match the from and two nodes. If these Matcher return multiple nodes, then the factory creates a Relation for the all the pairs in the
    kartesian product between the set that the from_matcher returns and the set that the to_matcher returns.
    """

    def __init__(self, attributes: List[AttributeFactory], type: AttributeFactory, from_matcher: Matcher, to_matcher: Matcher, primary_key: str = None, identifier: str = None) -> None:
        """Inits an NodeFactory with the following arguments
        
        Args:
            attributes: List of AttributeFactories for constructing the attributes for the relationship
            type: An AttributeFactory for constructing the type for the relationship
            from_matcher: Matcher object that returns the from-nodes for the relations
            to_matcher: Matcher object that return the to-nodes for the relations
            identifier: A string identifying this Factory instance. Can be None if factory doesn't need to save supplies
            primary_key: Optional key of the primary attribute. Used to merge the produced node with existing nodes in the graph (default: None)

        """
        super().__init__(identifier)
        self._attributes = attributes
        self._type = type
        self._from_matcher = from_matcher
        self._to_matcher = to_matcher
        self._primary_key = primary_key

    def construct(self, resource: Resource) -> Subgraph:
        """Constructs one or more Relations from a resource based on the the label/attribute factories and the from and to matchers provided
        when the instance was initialised.

        If the resource is None, then this method will return an empty Subgraph.

        Args:
            resource: A Resource containing any information needed for the construction
        Returns:
            A Subgraphs consisting of the constructed Relations
        """
        if resource is None:
            return Subgraph()
        from_nodes = self._from_matcher.match(resource)
        to_nodes = self._to_matcher.match(resource)
        rel_type = self._type.construct(resource)
        logger.debug(f"For relation type {rel_type.value} matched {len(from_nodes)} from_nodes and {len(to_nodes)} to nodes")
        attributes = [attr_factory.construct(resource) for attr_factory in self._attributes]
        attributes = [attr for attr in attributes if attr is not None]
        relations = Subgraph()
        for from_node in from_nodes:
            for to_node in to_nodes:
                relation = Relationship.from_attributes(from_node, rel_type, to_node, attributes)
                relation.__primarykey__ = self._primary_key
                relations = relations | relation
        return relations


@register_factory
class SupplyChain(SubgraphFactory):
    """Represents a chain of factories that are processed after eachother
    
    Attributes:
        factories: The list of factories in the supplychain
    """

    def __init__(self,  factories: List[SubgraphFactory] = None, identifier: str = None) -> None:
        """Inits a SupplyChain with factories
        
        Be aware that the order of the factories matters, since they are processed in order.

        Args:
            factories: (default: None) A list of factories
        """
        super().__init__(identifier)
        self._factories = factories

    @property
    def factories(self) -> List[SubgraphFactory]:
        """The list of factories in the supplychain"""
        return self._factories
    
    def append_factory(self, factory: SubgraphFactory) -> None:
        """Appends a factory to the end of the supply chain
        
        Args:
            factory: A factory"""
        self._factories.append(factory)

    def construct(self, resource: Resource) -> Subgraph:
        """Constructs a Subgraph by running all the factories in the supplychain in order.
        
        If the resource is None, then this method will return an empty Subgraph.

        Args:
            resource: A Resource containing any information needed for the construction
        Returns:
            A Subgraph
        """
        subgraph = Subgraph()

        if resource is not None:
            for factory in self._factories:
                product = factory.construct(resource)
                # if factory is registered, register supplies for the next factory
                if factory.id is not None and (len(product.relationships) > 0 or len(product.nodes) > 0):
                    resource.supplies[factory.id] = product
                subgraph = subgraph | product

        return subgraph


