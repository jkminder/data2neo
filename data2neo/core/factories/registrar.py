#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Keeps track of all registered factories. Allows to get a factory type by name as well as registering 
new factories.

Note: To circumvent problems with circular imports all typing in this file is made with Forward Hints (PEP 484: https://www.python.org/dev/peps/pep-0484/#forward-references)

authors: Julian Minder
"""

from enum import Enum, auto
import logging
from functools import wraps, partial

logger = logging.getLogger(__name__)

_registry = {}

def register_factory(factory):
    """A decorater that registers a Factory into the registry, such that it can be initiated dynamically with its name. 
    To initiate a registered factory check out get_factory. 

    Args:
        factory: The factory type to be stored in the registry
    Returns:
        The factory type itself
    """

    _registry[factory.__name__] = factory
    logger.debug(f"Registered factory '{factory.__name__}'.")
    return factory

def get_factory(name: str):
    """Gets a factory by name given the correct types from the registry.
    
    Usage:
        myfactory = get_factory("AttributeFactory")(InitParam1, InitParam2)
        mywrapper = get_factory("AttributeFactoryWrapper")(myfactory)

    Args:
        name: The name of the factory
    Returns:
        The factory type itself.
    Raises:
        KeyError: When the requested factory is not existing in the registry
    """
    try:
        logger.debug(f"Requested factory '{name}' from registry.")
        ret = _registry[name]
    except KeyError:
        raise KeyError(f"The requested module/factory '{name}' is not existing in the registry. Make sure to register your custom modules.")
    return ret

def func_attr(func, args, attribute):
    return func(attribute, *[arg.static_attribute_value for arg in args])

def wrap_post(func, wraptype):
    @wraps(func)
    def wrapped(factory, *args):
        return get_factory(wraptype)(factory, None, partial(func_attr, func, args))
    return wrapped

def wrap_pre(func, wraptype):
    @wraps(func)
    def wrapped(factory, *args):
        return get_factory(wraptype)(factory, partial(func_attr, func, args), None)
    return wrapped



def register_attribute_postprocessor(function):
    """Registers an attribute postprocessor"""
    # TODO Slightly ugly fix here since the config parser converts all string arguments to 
    # Static attribute factories. We need to convert them back
    if function.__name__ in _registry:
        logger.warning(f"The name '{function.__name__}' is already registered. Overwriting it.")
    logger.debug(f"Registered attribute postprocessor '{function.__name__}''.")
    _registry[function.__name__] = wrap_post(function, "AttributeFactoryWrapper")
    #lambda factory, *args: get_factory("AttributeFactoryWrapper")(factory, None, lambda attribute: function(attribute, *[arg.static_attribute_value for arg in args]))
    return function

def register_attribute_preprocessor(function):
    """Registers an attribute preprocessor"""
    # TODO Slightly ugly fix here since the config parser converts all string arguments to 
    # Static attribute factories. We need to convert them back
    if function.__name__ in _registry:
        logger.warning(f"The name '{function.__name__}' is already registered. Overwriting it.")
    logger.debug(f"Registered attribute preprocessor '{function.__name__}'.")
    _registry[function.__name__] =  wrap_pre(function, "AttributeFactoryWrapper")

#    _registry[function.__name__] = lambda factory, *args: get_factory("AttributeFactoryWrapper")(factory, lambda resource: function(resource, *[arg.static_attribute_value for arg in args]), None)
    return function

def register_subgraph_postprocessor(function):
    """Registers a subgraph postprocessor"""
    # TODO Slightly ugly fix here since the config parser converts all string arguments to 
    # Static attribute factories. We need to convert them back
    if function.__name__ in _registry:
        logger.warning(f"The name '{function.__name__}' is already registered. Overwriting it.")
    logger.debug(f"Registered subgraph postprocessor '{function.__name__}'.")
    _registry[function.__name__] =  wrap_post(function, "SubgraphFactoryWrapper")

    #_registry[function.__name__] = lambda factory, *args: get_factory("SubgraphFactoryWrapper")(factory, None, lambda attribute: function(attribute, *[arg.static_attribute_value for arg in args]))
    return function

def register_subgraph_preprocessor(function):
    """Registers a subgraph preprocessor"""
    # TODO Slightly ugly fix here since the config parser converts all string arguments to 
    # Static attribute factories. We need to convert them back.
    if function.__name__ in _registry:
        logger.warning(f"The name '{function.__name__}' is already registered. Overwriting it.")
    logger.debug(f"Registered subgraph preprocessor '{function.__name__}'.")
    #_registry[function.__name__] = lambda factory, *args: get_factory("SubgraphFactoryWrapper")(factory, lambda resource: function(resource, *[arg.static_attribute_value for arg in args]), None)
    _registry[function.__name__] =  wrap_pre(function, "SubgraphFactoryWrapper")
    return function

def register_wrapper(wrapper):
    """Registers a full wrapper"""
    # TODO Slightly ugly fix here since the config parser converts all string arguments to 
    # Static attribute factories. We need to convert them back.
    if wrapper.__name__ in _registry:
        logger.warning(f"The name '{wrapper.__name__}' is already registered. Overwriting it.")
    logger.debug(f"Registered wrapper '{wrapper.__name__}'.")
    _registry[wrapper.__name__] = lambda factory, *args: wrapper(factory,  *[arg.static_attribute_value for arg in args])
    return wrapper
