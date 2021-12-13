#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Implementation for odata as the relational module. 

authors: Julian Minder
"""

from typing import List
from .. import ResourceIterator
from .. import Resource
from pyodata.v2.service import EntityProxy




class ODataResource(Resource):
    """Implementation of the oData Resource. Enables access to an odata entity"""

    def __init__(self, entity: EntityProxy) -> None:
        super().__init__()
        self._entity = entity
    
    @property
    def type(self):
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

    @property
    def odata_entity(self):
        """Returns the oData Entity behind the resource"""
        return self._entity
    
class ODataListIterator(ResourceIterator):
    """Implements a Iterator that works based on a list of oData Entities."""

    def __init__(self, entities: List[EntityProxy]) -> None:
        super().__init__()
        self._entities = [ODataResource(entity) for entity in entities]
        self._i = -1

    def next(self) -> Resource:
        """Gets the next resource. Returns None if the range is traversed"""
        self._i += 1
        if self._i >= len(self._entities):
            return None
        return self._entities[self._i]
    
    def reset_to_first(self) -> None:
        """Resets the iterator to point to the first element"""
        self._i = -1