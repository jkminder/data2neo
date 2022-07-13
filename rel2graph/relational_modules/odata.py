#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Implementation for odata as the relational module. 

authors: Julian Minder
"""

from typing import Iterable, List
from .. import ResourceIterator
from .. import Resource
from pyodata.v2.service import EntityProxy




class ODataResource(Resource):
    """Implementation of the oData Resource. Enables access to an odata entity"""

    def __init__(self, entity: EntityProxy) -> None:
        super().__init__()
        self._entity = entity
    
    @property
    def type(self) -> str:
        """Returns the type of the resource. Is used to select correct factory"""
        return self._entity.entity_set.name

    def __getitem__(self, key):
        """
        Gets the value with key 'key'. 
        """
        return getattr(self._entity, key)
    
    def __setitem__(self, key, value):
        """
        Sets the value of with key 'key'.
        """
        setattr(self._entity, key, value)

    def __repr__(self) -> str:
        """
        Gets a string representation of the resource. Only used for logging.
        """
        return f"{super().__repr__()} {self._entity}"

    @property
    def odata_entity(self):
        """Returns the oData Entity behind the resource"""
        return self._entity
    
class ODataListIterator(ResourceIterator):
    """Implements a Iterator that works based on a list of oData Entities."""

    def __init__(self, entities: List[EntityProxy]) -> None:
        super().__init__()
        self._entities = [ODataResource(entity) for entity in entities]

    def __iter__(self) -> Iterable:
        """Returns the iterator itself in its initial state (must return the first resource)."""
        return iter(self._entities)

    def __len__(self) -> None:
        """Returns the total amount of resources in the iterator"""
        return len(self._entities)