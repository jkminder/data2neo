#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Factory Wrappers that allow to insert functionality before or after a factory. Allows generic extension of functionality.
A Factory Wrapper itself can be wrapped in a Wrapper.

authors: Julian Minder
"""

from abc import ABC, abstractmethod
from typing import Callable


from .resource import Resource
from ...neo4j.graph_elements import Attribute, GraphElement, Subgraph
from .factory import AttributeFactory, Factory, SubgraphFactory
from .registrar import register_factory


class FactoryWrapper(Factory, ABC):
    """Abstract FactoryWrapper that allows to insert pre and postprocessor functions that are called before and after 
    .construct of the wrapped factory. 
    
    Attributes:
        factory: The wrapped factory
    """
    @abstractmethod
    def __init__(self, factory: Factory, 
                 preprocessor: Callable[[Resource], Resource] = None,
                 postprocessor: Callable[[GraphElement], GraphElement] = None) -> None:
        """Inits the FactoryWrapper with an factory and a post and preprocessor
        
        Args:
            factory: The factory that should be wrapped
            preprocessor: A callable that takes a Resource, processes it and returns a resource (which is then passed to the factory). (default: None) 
            postprocessor: A callabel that takes the GraphElement, processes it and returns another GraphElement. (default: None) 
        """
        self._factory = factory
        self._preprocessor = preprocessor
        self._postprocessor = postprocessor

    @property
    def factory(self) -> Factory:
        """The wrapped factory"""
        return self._factory

    @abstractmethod
    def construct(self, resource: Resource) -> GraphElement:
        """Runs the preprocessor on the resource, uses the factory to construct an GraphElement from the resource
        and runs the postprocessor on this GraphElement

        Args:
            resource: A Resource containing any information needed for the construction       
        """
        processed_resource = resource
        if self._preprocessor is not None:
            processed_resource = self._preprocessor(resource)
        graph_element =  self.factory.construct(processed_resource)
        if self._postprocessor is not None:
            graph_element = self._postprocessor(graph_element)
        return graph_element
    

@register_factory
class SubgraphFactoryWrapper(FactoryWrapper, SubgraphFactory):
    """Factory Wrapper for any SubgraphFactory. Allows to insert pre and post processor functions that are called on the 
    resource/subgraph respectivelly before and after the .construct function of the wrapped factory. This factory behaves like 
    a normal SubgraphFactory and can be wrapped again.
    
    Attributes:
        factory: The wrapped SubgraphFactory
    """

    def __init__(self, factory: SubgraphFactory, preprocessor: Callable[[Resource], Resource] = None, postprocessor: Callable[[Subgraph], Subgraph] = None, identifier: str = None) -> None:
        """Inits the SubGraphFactoryWrapper with an factory and a post and preprocessor
        
        Args:
            identifier: A string identifying this Factory instance, must be unique
            factory: The SubgraphFactory that should be wrapped
            preprocessor: A callable that takes a Resource, processes it and returns a resource (which is then passed to the factory). (default: None) 
            postprocessor: A callabel that takes the Subgraph, processes it and returns another Subgraph. (default: None)
            identifier: A string identifying this Factory instance, must be unique. Can be None if factory doesn't need to save unique supplies

        """
        # super will call the First Parent class (first element in mro) -> FactoryWrapper init
        FactoryWrapper.__init__(self, factory, preprocessor=preprocessor, postprocessor=postprocessor)
        SubgraphFactory.__init__(self, identifier if identifier is not None else factory.id)

    def construct(self, resource: Resource) -> Subgraph:
        """Runs the preprocessor on the resource, uses the factory to construct an SubGraph from the resource
        and runs the postprocessor on this SubGraph.

        If resource is None then this method returns an emtpy SubGraph

        Args:
            resource: A Resource containing any information needed for the construction
        """
        if resource is None:
            return Subgraph()
        return FactoryWrapper.construct(self, resource)


@register_factory
class AttributeFactoryWrapper(FactoryWrapper, AttributeFactory):
    """Factory Wrapper for any AttributeFactory. Allows to insert pre and post processor functions that are called on the 
    resource/attribute respectivelly before and after the .construct function of the wrapped factory. This factory behaves like
    a normal AttributeFactory and can be wrapped again.
    
    Attributes:        
        identifier: A string identifying this Factory instance, must be unique
        factory: The wrapped AttributeFactory
        attribute_key: The key that any produced Attribute has.
        static_attribute_value: If this is set, then any attribute produced by this factory will have this string as its value
        entity_attribute: A key of an attribute of the expected resource entity.
    """

    def __init__(self, factory: AttributeFactory, preprocessor: Callable[[Resource], Resource] = None, postprocessor: Callable[[Attribute], Attribute] = None, identifier: str = None) -> None:
        """Inits the AtttributeFactoryWrapper with an factory and a post and preprocessor
        
        Args:
            factory: The AttributeFactory that should be wrapped
            preprocessor: A callable that takes a Resource, processes it and returns a resource (which is then passed to the factory). (default: None) 
            postprocessor: A callabel that takes the Attribute, processes it and returns another Attribute. (default: None) 
            identifier: A string identifying this Factory instance, must be unique. Can be None if factory doesn't need to save unique supplies
        """
        FactoryWrapper.__init__(self, factory, preprocessor=preprocessor, postprocessor=postprocessor)
        AttributeFactory.__init__(self, self.factory.attribute_key, self.factory.entity_attribute, self.factory.static_attribute_value, identifier if identifier is not None else factory.id)


    def construct(self, resource: Resource) -> Attribute:
        """Runs the preprocessor on the resource, uses the factory to construct an Attribute from the resource
        and runs the postprocessor on this Attribute.

        If resource is None then this method returns None

        Args:
            resource: A Resource containing any information needed for the construction
        """
        if resource is None:
            return None
        return FactoryWrapper.construct(self, resource)

