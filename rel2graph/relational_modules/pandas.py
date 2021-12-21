#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Implementation for odata as the relational module. 

authors: Julian Minder
"""

from typing import List
from .. import ResourceIterator
from .. import Resource
import pandas as pd



class PandasDataframeResource(Resource):
    """Implementation of the oData Resource. Enables access to an odata entity"""

    def __init__(self, series: pd.core.series.Series, type: str) -> None:
        super().__init__()
        self._series = series
        self._type = type
        self._changed_values = {}

    @property
    def type(self):
        """Returns the type of the resource. Is used to select correct factory"""
        return self._type

    def __getitem__(self, key):
        """
        Gets the value with key 'key'. 
        """
        # We make sure that we don't change the initial series and keep track of changes in the resource
        if key in self._changed_values.keys():
            return self._changed_values[key]
        return self._series[key]
    
    def __setitem__(self, key, value):
        """
        Sets the value of with key 'key'.
        """
        # We make sure that we don't change the initial series and keep track of changes in the resource
        self._changed_values[key] = value

    def __repr__(self) -> str:
        """
        Gets a string representation of the resource. Only used for logging.
        """
        return f"{super().__repr__()} (row {self._series.name})"

class PandasDataframeIterator(ResourceIterator):
    """Implements a Iterator that works based on a list of oData Entities."""

    def __init__(self, dataframe: pd.core.frame.DataFrame, type: str) -> None:
        super().__init__()
        self._rows = [PandasDataframeResource(dataframe.loc[i], type) for i in range(len(dataframe))]
        self._i = -1

    def next(self) -> Resource:
        """Gets the next resource. Returns None if the range is traversed"""
        self._i += 1
        if self._i >= len(self._rows):
            return None
        return self._rows[self._i]
    
    def reset_to_first(self) -> None:
        """Resets the iterator to point to the first element"""
        self._i = -1
    
    def __len__(self) -> None:
        """Returns the total amount of resources in the iterator"""
        return len(self._rows)