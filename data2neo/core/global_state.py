#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module encoding the global state that is shared between all process workers.

authors: Julian Minder
"""

from typing import Any

class __DynamicGetter(type):
    _custom_global_vars = {}

    def __getattr__(self, name):
        if name == "_custom_global_vars":
            return super().__getattr__(name)
        if name == "graph_driver":
            return self.__driver
        if name in self._custom_global_vars:
            return self._custom_global_vars[name]
        else:
            raise AttributeError("No such global variable: " + name)

    def __setattr__(self, __name: str, __value: Any) -> None:
        if __name in ["_custom_global_vars", "graph_driver"]:
            raise AttributeError(f"Cannot set {__name}. Forbidden attribute.")
        self._custom_global_vars[__name] = __value
    
    def __delattr__(self, __name: str) -> None:
        del self._custom_global_vars[__name]

    def keys(self):
        return self._custom_global_vars.keys()

    def _set_graph_driver(self, driver):
        self.__driver = driver

    def _del_graph_driver(self):
        del self.__driver

    def get_state(self):
        return self._custom_global_vars
    
    def set_state(self, state):
        for key, value in state.items():
            self._custom_global_vars[key] = value

class GlobalSharedState(metaclass=__DynamicGetter):
    pass
