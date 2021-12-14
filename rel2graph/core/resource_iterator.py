#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Represents iterator objects that are used by the converter. Also includes 
an iteratoriterator that allows to bundle multiple iterators together.

authors: Julian Minder
"""
from abc import ABC, abstractmethod
from typing import List
from .factories.resource import Resource

class ResourceIterator(ABC):
    """Allows the Converter to iterate over objects. It allows to iterate over the same range twice."""

    @abstractmethod
    def __init__(self) -> None:
        pass

    @abstractmethod
    def next(self) -> Resource:
        """Gets the next resource that will be converted. Returns None if the range is traversed."""
        pass
    
    @abstractmethod
    def reset_to_first(self) -> None:
        """Resets the iterator to point to the first element"""
        pass

    @abstractmethod
    def __len__(self) -> None:
        """Returns the total amount of resources in the iterator"""
        pass

class IteratorIterator(ResourceIterator):
    """Allows to Iterator over a list of Iterators"""

    def __init__(self, iterators: List[ResourceIterator]) -> None:
        """Initialises an IteratorIterator.
        
        Args:
            iterators: List of ResourceIterators
        """
        super().__init__()
        self._iterators = iterators
        self._i = 0

    def next(self) -> Resource:
        """Gets the next resource that will be converted. Returns None if the range is traversed."""
        if self._i >= len(self._iterators):
            return None
        next_resource = self._iterators[self._i].next()
        if next_resource is None:
            self._i += 1
            if self._i >= len(self._iterators):
                return None
            else:
                next_resource = self._iterators[self._i].next()
        return next_resource
    
    def reset_to_first(self) -> None:
        """Resets the iterator to point to the first element"""
        self._i = 0
        for iterator in self._iterators:
            iterator.reset_to_first()

    def __len__(self) -> None:
        """Returns the total amount of resources in the iterator"""
        total = 0
        for iterator in self._iterators:
            total += len(iterator)
        return total