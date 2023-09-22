#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Implementation for pandas as the relational module. 

authors: Julian Minder
"""

from typing import List, Iterable, Dict
from .. import ResourceIterator
from .. import Resource
import pandas as pd
import warnings


class PandasSeriesResource(Resource):
    """Implementation of the pandas Resource. Enables access to an pandas series"""

    def __init__(self, series: pd.core.series.Series, type: str) -> None:
        """
        Wraps a pandas series and is used as input data object for the converter. May hold additional supplies to pass data between factories.
        
        Args:
            series: Wrapped pandas series.
            type: Name of type that this series is an entity of.
        """
        super().__init__()
        self._series = series
        self._type = type
        self._changed_values = {}

    @property
    def type(self) -> str:
        """Returns the type of the resource. Is used to select correct factory"""
        return self._type

    @property
    def series(self) -> Dict:
        """Gets the wrapped pandas series"""
        return self._series
        
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
    """DEPRECATED: replaced by PandasDataFrameIterator"""

    def __init__(self, *args) -> None:
        warn_msg = "PandasDataframeIterator has been deprecated and replaced by PandasDataFrameIterator.\
            Please update your code accordingly."
        warnings.warn(warn_msg)
        raise DeprecationWarning(warn_msg)

class PandasDataFrameIterator(ResourceIterator):
    """Implements a Iterator that works based on a list of pandas Entities."""

    def __init__(self, dataframe: pd.core.frame.DataFrame, type: str) -> None:
        super().__init__()
        self._rows = [PandasSeriesResource(dataframe.iloc[i], type) for i in range(len(dataframe))]

    def __iter__(self) -> Iterable:
        """Returns the iterator itself in its initial state (must return the first resource)."""
        return iter(self._rows)
    
    def __len__(self) -> None:
        """Returns the total amount of resources in the iterator"""
        return len(self._rows)
