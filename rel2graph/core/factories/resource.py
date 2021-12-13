#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Implements the abstract factory resource. A resource represents everything a factory needs to produce its output. It primarily abstracts access to an entity. 
It may contain additional supplies produced by other factories.

authors: Julian Minder
"""

from abc import ABC, abstractmethod

class Resource(ABC):
    """Abstract Factory Resource container
    
    Contains everything a factory needs to produce its output. 
    """
    def __init__(self) -> None:
        """Inits a Resource
        """
        self._supplies = {}

    @property
    def supplies(self):
        """Returns access to supplies from past factories"""
        return self._supplies

    @property
    @abstractmethod
    def type(self):
        """Returns the type of the resource. Is used to select correct factory"""
        pass

    @abstractmethod
    def __getitem__(self, key):
        """
        Gets the value with key 'key'. 
        """
        pass
    
    @abstractmethod
    def __setitem__(self, key, value):
        """
        Sets the value of with key 'key'.
        """
        pass

    def clear_supplies(self):
        """Clears the supplies"""
        self._supplies.clear()


    