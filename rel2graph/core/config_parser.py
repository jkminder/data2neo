#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Classes and methods for parsing and compiling the config document into factories.

A file is first read and precompiled and converted to a YAML compatible format. After that it is decoded based on the YAML specifications.
Each entity is then again first precompiled into a string that represents exact Constructors for Objects. These strings are then parsed into instructions, which 
is a list of [modulename, attribute] pairs (where attributes can be instructions as well). Finally the instructions are then compiled into factories.

authors: Julian Minder
"""

import logging
import re
from yaml import load, FullLoader

from .factories.registrar import get_factory
from typing import Any, List, Tuple

logger = logging.getLogger(__name__)

class ConfigError(ValueError):
    """Exception for specific config errors"""
    pass


"""Represent rules that are applied to the config file for reformating certain modules"""
_config_module_reformat_rules = [
    ("NODE", "NodeFactory({{attributes}},[{args}],{{primary_key}}, \"{{parent}}.{{id}}\")"),
    ("RELATION", "RelationFactory({{attributes}},{1},{0},{2})"),
    ("MATCH", "Matcher(&None, {args})")
]


def _split_arguments(arguments: str) -> List[str]:
    """Splits a string into individual arguments by counting the opening and closing brackets.
    
    E.g. splits '"abc", functor("args")' into ["abc", functor("args")]

    Args:
        arguments: Arguments as a string
    Returns:
        A list of arguments
    Raises:
        ValueError: If the number of opening and closing brackets is not the same
    """
    splitted = []
    level = 0
    first = -1
    for i in range(len(arguments)):
        if arguments[i] not in ["(", ")", "[", "]", ",", " "] and first < 0:
            first = i
        elif arguments[i] in ["(", "["] and (i < 1 or arguments[i-1]!="\\"):
            level += 1
            if first < 0:
                first = i
        elif arguments[i] in [")", "]"] and (i < 1 or arguments[i-1]!="\\"):
            level -= 1
        # end an argument
        if arguments[i] == "," and level == 0:
            splitted.append(arguments[first:i])
            first = -1
        # last argument
        if i == len(arguments)-1:
            if level == 0:
                splitted.append(arguments[first:])
            else:
                raise ValueError(f"The arguments list has not the same number of opening and closing brackets: {arguments}")
    return splitted

def _index_of_closing_bracket(string: str, start: int) -> int:
    """Calculates the index of the closing bracket of the first encountered opening bracket in string from position start.
    
    Args:
        string: Input string
        start: Start position in string from we search
    Returns:
        The index of the closing bracket
    Raises:
        ValueError: If closing bracket could not be found
    """
    level = 0
    for i in range(start, len(string)):
        if string[i] in ["(", "["] and (i < 1 or string[i-1]!="\\"):
            level += 1
        elif string[i] in [")", "]"] and (i < 1 or string[i-1]!="\\"):
            level -= 1
            if (level == 0):
                return i
    raise ValueError("Could not find closing bracket")
    
def _parse_module(config_str: str, module_name: str) -> List[str]:
    """Extracts the arguments of a module in a string as a list and returns the start and end index 
    of the module specifications.
    
    E.g. _parse_module("Test(One(NODE(arg1, arg2)))", "NODE") -> [arg1, arg2], 9, 30

    Args:
        config_str: String to search in
        module_name: name of the module
    Returns:
        if the config str does not contain the module: None, None, None
        else:
        The arguments as a list, The module specification start index, The module specification end index (closing bracket)
    """
    start = config_str.find(module_name)
    if start == -1:
        return None, None, None
    end = _index_of_closing_bracket(config_str, start)
    return _split_arguments(config_str[start+len(module_name)+1:end]), start, end

def _convert(match: re.Match, target_pattern: str) -> str:
    """Helper function for formating a target_pattern based on a match"""
    return target_pattern.format(*match.groups())

def _string_to_instructions(config_str: str) -> List[Any]:
    """Converts a string into a list of instructions that then can be compiled. The list contains
    for each module a pair (module, args)
    
    E.g. converts "Node(Test(arg1), arg2)" into ["Node", [["Test", [arg1]], arg2]]

    Args:
        config_str: string to convert
    Returns:
        The parsed instructions
    """
    instructions = []
    end = 0
    end = config_str.find("(")
    while(end > 0 and config_str[end-1] == "\\"):
        end = config_str[end+1:].find("(")

    end_e = config_str.find("[")
    while(end_e > 0 and config_str[end_e-1] == "\\"):
        end_e = config_str[end_e+1:].find("[")
    if end_e >= 0 and end > end_e:
        # unpack list
        args = _split_arguments(config_str[end_e+1:-1]) # because we need to remove the closing bracket
        arg_instructions = []
        for arg in args:
            arg_instructions.append(_string_to_instructions(arg))
        instructions.extend(arg_instructions)
    elif end > 0:
        instructions.append(config_str[:end])
        args = _split_arguments(config_str[end+1:-1]) # because we need to remove the closing bracket

        arg_instructions = []
        for arg in args:
            arg_instructions.append(_string_to_instructions(arg))
        instructions.append(arg_instructions)
    elif end_e >= 0 and len(config_str) == 2:
        # Handle empty list of args
        args = _split_arguments(config_str[end_e+1:-1]) # because we need to remove the closing bracket
        instructions.extend(args)
    else:
        return config_str.strip("\"").replace("\\", "") if config_str != 'None' else None
    return instructions

def _parse_to_instructions(config_data: List[str]) -> Tuple[List[Any], List[Any]]:
    """Converts the config data (a list of module strings) into instructions and splits them into
    nodes and relations.
    
    Args:
        config_data: A list containing module config strings
    Returns:
        A Tuple (node_instructions, relation_instructions) where each instruction is a nested list of strings
    """
    node_instructions = []
    relation_instructions = []
    for graph_element_config in config_data:
        if "NodeFactory" in graph_element_config:
            node_instructions.append(_string_to_instructions(graph_element_config))
        elif "RelationFactory" in graph_element_config:
            relation_instructions.append(_string_to_instructions(graph_element_config))
        else:
            raise ValueError(f"Unknown Type: {graph_element_config}")
    return node_instructions, relation_instructions

def _unravel_args(args: List[Any]) -> List[Any]:
    """Unravels arguments by compiling them if required.
    
    Args:
        args: List of argument instructions
    Returns:
        A compiled list of arguments
    """
    unraveled = []
    for arg in args:
        if isinstance(arg, list):
            unraveled.append(_compile(arg))
        else:
            unraveled.append(arg)
    return unraveled

def _compile_module(module: str, args: List) -> "Factory":
    """Compiles a module and args into a factory"""
    factory_args = _unravel_args(args)
    logger.debug(f"Compiling module '{module}' with static args {[arg for arg in factory_args if isinstance(arg, str)]}")
    return get_factory(module)(*factory_args)

def _compile(instructions: List) -> "Factory":
    """Recursively compiles a list of instructions
    
    Args:
        instructions: The instructions to compile
    Returns:
        A compiled factory
    """
    # Check if for single or nested module
    if len(instructions) == 0:
        return []
    if isinstance(instructions[0], str):
        module, args = instructions
        return _compile_module(module, args)

    factories = []
    for module, args in instructions:
        factories.append(_compile_module(module, args))
    return factories

def _escape(config_str: str) -> str:
    """Escapes all relevant characters in a string"""
    escape_chars = "()[]"
    for char in escape_chars:
        config_str = config_str.replace(char, f"\{char}")
    return config_str

def _precompile_file(filestream):
    """
    Reads the full content of a filestream and apply general reformating rules the the string. 
    Is applied before decoding the string to yaml and makes sure that the syntax is YAML compatible
    """
    full_config_str = filestream.read()

    # Convert primary key syntax to a YAML compatible format 
    full_config_str = full_config_str.replace("+", "- +")

    return full_config_str

class ConfigEntityCompiler:
    """Compiles config data for an entity"""
    def __init__(self) -> None:
        self._saved_attributes = {}
        self._ids = set()

    def _precompile_static_nokey_arguments(self, config_str: str, allowed_presymbols=",=") -> str:
        """Converts static strings into AttributeFactory Constructor strings for static attributes
        
        Args:
            config_str: string to precompile
            allowed_presymbols: symbols that are allowed before a static argument as a string (concated together) (default = ",=")
        Returns:
            Precompiled string
        """
        return re.sub(f"([\([{allowed_presymbols}])\s*((\".*?\")||\d*)(?=\s*[]\),])", lambda match : "{0}AttributeFactory(&None,&None,&{1})".format(match.group(1), _escape(match.group(2))), config_str)

    def _precompile_dynamic_nokey_arguments(self, config_str: str, allowed_presymbols="[^\"=]") -> str:
        """Converts dynamic arguments either from entities or nodes into AttributeFactory Constructor strings.
        If the dynamic argument refers to a node it will copy the attribute string from the saved_attributes dict.
        
        Args:
            config_str: string to precompile
            allowed_presymbols: symbols that are allowed before a argument as a string (concated together) (default = ",=")

        Returns:
            Precompiled string
        """
        # Parse Dynamic Entity Attributes without key
        for match in re.finditer(f"{allowed_presymbols}\s*(\w*)[^&][.](\w*)", config_str):
            if match.group(1) == self._entity_type:
                config_str = re.sub(match.group(0), _convert(match, "AttributeFactory(&None,\"{1}\",&None)"), config_str)
            else:
                # TODO: Inefficient since dynamic attributes are recomputed and not extracted
                # Need structure to dynamically extract information from an existing node
                if match.group(0) not in self._saved_attributes.keys():
                    raise ConfigError(f'Parsing Error: "{match.group(0)}" must be defined before its refered to')
                config_str = re.sub(match.group(0), self._saved_attributes[match.group(0)], config_str)
        return config_str

    def _precompile_entity_type(self, config_str: str) -> str:
        """Precompiles the entity type"""
        match = re.search("ENTITY\(\"(\w+)\"\)", config_str)
        if match is None:
            raise ConfigError("Parsing Error: Could not locate 'ENTITY(\"entity_type\")'")
        self._entity_type = match.group(1)
        
    def _precompile_graph_element(self, config_str: str) -> str:
        """Precompiles a graph element (Node or Relation)"""
        # Convert ids into Matchers
        for id in self._ids:
            config_str = re.sub(f"{id}(?!\s*$)", f"Matcher(&\"{{parent}}&.{id}\")", config_str)
        
        # Parse Static Argument Attributes (no key)
        config_str = self._precompile_static_nokey_arguments(config_str)

        # Parse Dynamic Attributes for nodes with key    
        config_str = re.sub("\s*(\w*)\s*[=]\s*((\w*\()*)\s*(\w+)[.](\w*)", lambda match : _convert(match, "{1}AttributeFactory(&\"{0}\",&\"{4}\",&None)"), config_str)

        # Parse Dynamic Arguments (no key)
        config_str = self._precompile_dynamic_nokey_arguments(config_str)

        # Parse MATCH keywords (require specific handling due to args and kwargs)
        #match_args, match_start, match_end = _parse_module(config_str, "MATCH")
        #if match_start is not None:
            #config_str = config_str[:match_start] + "Matcher(&None," + ",".join(match_args) + config_str[match_end:]

        # Parse main element
        for module, rule in _config_module_reformat_rules:
            arguments, start, end = _parse_module(config_str, module)
            while(True):
                if arguments is None:
                    break
                # A node can contain an id, that needs to be removed -> search last closing bracket
                end_of_module_specification = _index_of_closing_bracket(config_str, 0)
                config_str = config_str[:start] + rule.format(*arguments, args=",".join(arguments)) + config_str[end+1:end_of_module_specification+1]
                arguments, start, end = _parse_module(config_str, module)
        return config_str

    def _precompile_attribute(self, config_str: str) -> str:
        """Precompiles an attribute"""
        # Test if is primary key attribute
        match = re.search("\s*[+](.*)", config_str)
        primary = False
        if match is not None:
            primary = True
            config_str = match.group(1) # remove primary marker (+)
        

        # Parse Static Attributes (with key)
        key = re.search("\s*(\w*)\s*[=]", config_str).group(1)
        config_str = re.sub("\s*(\w*)\s*[=]\s*((\w*\()*)\s*(\".*?\")(.*)", lambda match : _convert(match, "{1}AttributeFactory(&\"{0}\",&None,&{3}){4}"), config_str)
        
        # Parse Static arguments (no key)  
        config_str = self._precompile_static_nokey_arguments(config_str)

        # Parse Dynamic Attributes for nodes with key    
        config_str = re.sub("\s*(\w*)\s*[=]\s*((\w*\()*)\s*(\w+)[.](\w*)(.*)", lambda match : _convert(match, "{1}AttributeFactory(&\"{0}\",&\"{4}\",&None){5}"), config_str)

        # Parse Dynamic Arguments (no key)
        config_str = self._precompile_dynamic_nokey_arguments(config_str)

        return config_str, key, primary

    def compile(self, entity_type, config_data):
        """Compiles config data into factories. 
        
        Args:
            entity_type: String of the entity_type
            config_data: The config data of the refered entity_type
        Returns:
            A tuple (entity_type_name, (NodeSupplyChain, RelationSupplyChain))"""
        logger.debug(f"Compiling {entity_type} : {config_data}")
        # Handle top level elements (Nodes and Relations)
        self._precompile_entity_type(entity_type)
        
        precompiled_entity = []

        for element_config, attributes in config_data.items():
            # Grab id
            match = re.search("[\)]\s*(\w+)\s*$", element_config)
            id = None
            if match is not None:
                id = match.group(1)
                self._ids.add(id)
            
            # Precompile attributes
            precompiled_attributes = []
            primary_key = "&None"
            if attributes is not None:
                for attribute in attributes:
                    config, key, primary = self._precompile_attribute(attribute)
                    precompiled_attributes.append(config)
                    # if an id exist we save this attribute for later reference
                    if id is not None:
                        self._saved_attributes[f"{id}.{key}"] = config
                    
                    # if primary key we save it
                    if primary:
                        if primary_key == "&None":
                            primary_key = key
                        else:
                            raise ConfigError(f"Error in config for entity '{self._entity_type}'{' in graph element with identifier - ' + id if id is not None else ''}: Only 1 primary key allowed")
            precompiled_graph_element = self._precompile_graph_element(element_config)
            
            # Add attributes to the string
            precompiled_attributes_str = "[" + ",".join(precompiled_attributes) + "]"
            precompiled_graph_element = precompiled_graph_element.format(attributes=precompiled_attributes_str, parent=self._entity_type, id=id, primary_key = primary_key)

            # Remove compile markers
            precompiled_graph_element = precompiled_graph_element.replace("&", "")

            # append to list
            precompiled_entity.append(precompiled_graph_element)
        
        # Convert to instructions
        node_instructions, relation_instructions = _parse_to_instructions(precompiled_entity)

        # Compile instructions to factories
        node_factories, relation_factories = _compile(node_instructions), _compile(relation_instructions)
        return self._entity_type, (get_factory("SupplyChain")(node_factories, "NodeSupplyChain"), get_factory("SupplyChain")(relation_factories, "RelationSupplyChain"))



def parse(filename):
    """Parses a config file into usable factories.
    
    Args:
        filename: Filepath of the configfile
    Returns:
        A dict in form of (entity_type_name, (NodeSupplyChain, RelationSupplyChain))
        for all provided entity_types
    """
    logger.debug(f"Parsing config file '{filename}'")
    with open(filename, "r") as fstream:
        precompiled_string = _precompile_file(fstream)
        decoded_yaml = load(precompiled_string, FullLoader)
    compiled = {}
    
    for entity_type, entity_config in decoded_yaml.items():
        compiler = ConfigEntityCompiler()
        entity_type, factories = compiler.compile(entity_type, entity_config)
        compiled[entity_type] = factories
    return compiled