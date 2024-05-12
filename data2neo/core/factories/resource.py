#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Implements the abstract factory resource. A resource represents everything a factory needs
to produce its output. It primarily abstracts access to an entity.
It may contain additional supplies produced by other factories.

authors: Julian Minder
"""

from abc import ABC, abstractmethod
from typing import Dict

class Resource():
    """Abstract Resource class. Contains everything a factory needs to produce its output.
    This must be implemented.
    """

    def __init__(self) -> None:
        """Inits a Resource"""
        self._supplies = {}

    @property
    def supplies(self) -> Dict:
        """Returns access to supplies from past factories. Is used to pass data between factories. This should not be customised.
        """
        return self._supplies

    @property
    @abstractmethod
    def type(self) -> str:
        """Returns the type of the resource. Is used to select correct factory."""
    
    @abstractmethod
    def __repr__(self) -> str:
        """
        Gets a string representation of the resource. Only used for logging.

        Should follow the format:
        NameOfResource 'TypeOfResource' (DetailsAboutResource)

        Example-Implementation:
        f"{super().__repr__()} ({self.somedetail})"
        """
        return f"{self.__class__.__name__} '{self.type}'"

    @abstractmethod
    def __getitem__(self, key: str) -> str:
        """
        Gets the value with key 'key'.
        """

    @abstractmethod
    def __setitem__(self, key: str, value: str) -> None:
        """
        Sets the value of with key 'key'.
        """

    def clear_supplies(self) -> None:
        """Clears the supplies"""
        self._supplies.clear()

    