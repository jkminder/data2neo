#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Represents iterator objects that are used by the converter. Also includes 
an iteratoriterator that allows to bundle multiple iterators together.

authors: Julian Minder
"""
from abc import ABC, abstractmethod
from typing import List, Iterable
from .factories.resource import Resource
from itertools import chain
import warnings


class ResourceIterator(ABC):
    """Allows the Converter to iterate over resource. It allows to iterate over the same range twice."""

    @abstractmethod
    def __init__(self) -> None:
        pass
    
    def __next__(self) -> Resource:
        """Gets the next resource that will be converted. Raises StopIteration if the range is traversed."""
        raise NotImplementedError("__next__ is not implemented")
    
    @abstractmethod
    def __len__(self) -> int:
        """Returns the total amount of resources in the iterator"""
        pass
    
    @abstractmethod
    def __iter__(self) -> Iterable:
        """Returns the iterator itself in its initial state (must return the first resource)."""
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

    def __iter__(self) -> Iterable:
        """Returns the iterator itself"""
        return chain.from_iterable(self._iterators)

    def __len__(self) -> None:
        """Returns the total amount of resources in the iterator"""
        total = 0
        for iterator in self._iterators:
            total += len(iterator)
        return total