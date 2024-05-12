#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Classes and methods for parsing and compiling the config document into factories.

A file is first read and precompiled (all comments removed).
The string is then parsed with the ply yacc/lex implementation into instructions, which
is a list of [modulename, attribute] pairs (where attributes can be instructions as well). Finally the instructions are then compiled into factories.

authors: Julian Minder
"""

from typing import List, Any
from collections import Counter
import re
import logging
from numpy import extract
from ply import lex, yacc

from .factories.registrar import get_factory

logger = logging.getLogger(__name__)


class SchemaConfigException(ValueError):
    """Exception for syntax errors in the schema config file."""
    pass


class SchemaConfigParser:
    """
    Parses the schema config file into data2neo instructions that can directly be converted into factories and their arguments. Uses ply to
    parse and lex the grammar.

    All t_{name} methods/attributes of the class are used by lex to parser the document into tokens. All p_{name} functions define the grammar for 
    the schema file and are parsed by the yacc module. Check out the ply documentation for more information.
    """

    tokens = (
        'INT',
        'FLOAT',
        'STRING',
        'BOOL',
        'ENTITY',
        'NODE',
        'RELATIONSHIP',
        'MATCH',
        'NAME',
        'DOT',
        'COMMA',
        'EQUAL',
        'LPAR',
        'RPAR',
        'COLON',
        'SEP',
    )

    t_STRING = r'"(?:(?!"|\\).|\\.)*"|\'(?:(?!\'|\\).|\\.)*\''
    t_NAME = r'\b(?!\b(?:False|True|ENTITY|NODE|RELATIONSHIP|MATCH)\b)[a-zA-Z_]\w*\b'
    t_ENTITY = r'\bENTITY\b'
    t_NODE = r'\bNODE\b'
    t_RELATIONSHIP = r'\bRELATIONSHIP\b'
    t_MATCH = r'\bMATCH\b'
    t_DOT = r'\.(?!\d+\b)'
    t_COMMA = r'\,'
    t_EQUAL = r'\='
    t_COLON = r'\:'
    t_LPAR = r'\('
    t_RPAR = r'\)'
    t_SEP = r'[+-](?<!\w[+-])'

    def t_BOOL(self, t):
        r'\b(?:True|False)\b'
        t.value = t.value == "True"
        return t

    def t_INT(self, t):
        r'\b(?<!\.)\d+(?!\.)\b'
        t.value = int(t.value)
        return t

    def t_FLOAT(self, t):
        r'\b(?<!\.)\d+\.\d+\b'
        t.value = float(t.value)
        return t

    # Define a rule so we can track line numbers
    def t_newline(self, t):
        r'\n+'
        t.lexer.lineno += len(t.value)

    # A string containing ignored characters (spaces and tabs)
    t_ignore = ' \t'

    # Error handling rule
    def t_error(self, t):
        raise SchemaConfigException(f"Illegal character '{t.value[0]}' on line {t.lexer.lineno}\nCONTEXT: \n...{t.lexer.lexdata[max(0, t.lexpos-20):t.lexpos]}{t.value[0]}\u0332{t.value[1:min(50, len(t.value))]}\n...")

    def __init__(self):
        self.lexer = lex.lex(module=self)
        self.parser = yacc.yacc(module=self, debug=False)

        self._identifiers = [] # used to verify that an identifier is only used once per entity

    def tokenize(self, data):
        self.lexer.input(data)
        while True:
            tok = self.lexer.token()
            if not tok:
                break      # No more input
            print(tok)

    def parse(self, data):
        return self.parser.parse(data)

    def p_entities(self, p):
        '''entities : entity entities
                    | empty'''
        case = len(p)-1
        instructions = []
        if case == 2:
            instructions.append(p[1])
            if len(p[2]):
                instructions.extend(p[2])
        p[0] = instructions

    def p_entity(self, p):
        '''entity : ENTITY LPAR STRING RPAR COLON graphelements
        '''
        p[0] = [self._cleanup_string(p[3]), p[6]]
        # verify that every identifier is only used once per entity
        identifier_counts = Counter(self._identifiers)
        duplicated_identifiers = [key for key in identifier_counts.keys() if identifier_counts[key]>1]
        if len(duplicated_identifiers):
            raise SchemaConfigException(f"Found conflicting definitions of identifiers {duplicated_identifiers} in entity '{p[0][0]}'. An identifier must be unique.")
        # clear identifier list
        self._identifiers = []

    @staticmethod
    def _extract_key_from_attribute(attribute):
        if attribute[0] == "AttributeFactory":
            return attribute[1][0]
        else:
            return SchemaConfigParser._extract_key_from_attribute(attribute[1][0])
    
    @staticmethod
    def _inject_graphelement_args(instructions, attributes, identifier):
        """
        This function injects graphelement arguments (attributes, primary_key and identifier) into a list of instructions containing a graphelement. Due to
        possible wrapper definitions this requires recusively diving into the instructions. Further it returns a flag specifying if this graphelement represents a
        node.

        Returns:
            Instructions with injected arguments, flag if this is a node
        """
        if instructions[0] in ["NodeFactory", "RelationshipFactory"]:
            is_node = instructions[0] == "NodeFactory"
            # Find primary attribute and extract attributes
            raw_attributes = []
            primary_key = None
            for primary, attribute in attributes:
                raw_attributes.append(attribute)
                if primary:
                    if primary_key is not None:
                        raise SchemaConfigException(
                            f"Setting two or more primary keys for one graphelement is not allowed. Conflict: '{primary_key}' <-> '{attribute[1][0]}'")
                    primary_key = SchemaConfigParser._extract_key_from_attribute(attribute)
            instructions[1].insert(0, raw_attributes)
            instructions[1].extend([primary_key, identifier])
        else:
            instructions[1][0], is_node = SchemaConfigParser._inject_graphelement_args(
                instructions[1][0], attributes, identifier)
        return instructions, is_node

    def p_graphelements(self, p):
        '''graphelements : graphelement identifier COLON attributes graphelements
                         | empty'''
        case = len(p)-1
        instructions = [[], []]  # nodes, relationships
        if case == 5:
            # We need to inject the attributes into the correct location in the graph element instructions
            # The attributes are always the first argument for any graphelement
            graphelement, is_node = SchemaConfigParser._inject_graphelement_args(p[1], p[4], p[2])
            instructions[0 if is_node else 1].append(graphelement)

            instructions[0].extend(p[5][0])
            instructions[1].extend(p[5][1])

        p[0] = instructions

    def p_identifier(self, p):
        '''identifier : NAME
                      | empty'''
        p[0] = p[1]
        if p[0] is not None:
            self._identifiers.append(p[0])

    def p_graphelement(self, p):
        '''graphelement : node
                        | relationship
                        | NAME LPAR graphelement staticarguments RPAR'''
        case = len(p)-1
        instructions = []
        if case == 5:
            instructions = [p[1], [p[3], *p[4]]]
        else:
            instructions = p[1]
        p[0] = instructions

    def p_node(self, p):
        '''node : NODE LPAR arguments RPAR'''
        p[0] = ["NodeFactory", [p[3]]]

    def p_relationship(self, p):
        '''relationship : RELATIONSHIP LPAR destination COMMA argument COMMA destination RPAR'''
        p[0] = ["RelationshipFactory", [p[5], p[3], p[7]]]

    def p_destination(self, p):
        '''destination : NAME
                       | MATCH LPAR mixedarguments RPAR'''
        case = len(p)-1
        instructions = []
        if case == 1:
            instructions = ["Matcher", [p[1]]]
        else:
            instructions = ["Matcher", [None, *p[3]]]
        p[0] = instructions

    def p_staticvalue(self, p):
        '''staticvalue : STRING
                       | BOOL
                       | INT
                       | FLOAT'''
        p[0] = SchemaConfigParser._cleanup_string(p[1])

    def p_attributes(self, p):
        '''attributes : SEP attribute attributes
                      | empty'''
        case = len(p)-1
        instructions = []
        if case == 3:
            instructions = [(True if p[1] == '+' else False, p[2])]
            instructions.extend(p[3])

        p[0] = instructions

    @staticmethod
    def _inject_attribute_key(instructions, key):
        """
        This function injects attribute arguments (attribute key) into a list of instructions containing a attribute. Due to
        possible wrapper definitions this requires recusively diving into the instructions.
        """
        if instructions[0] in ["AttributeFactory"]:
            instructions[1][0] = key
        else:
            instructions[1][0] = SchemaConfigParser._inject_attribute_key(
                instructions[1][0], key)
        return instructions

    def p_attribute(self, p):
        '''attribute : NAME EQUAL argument'''
        instructions = p[3]
        # Set attribute key (requires recursion due to possible wrappers)
        SchemaConfigParser._inject_attribute_key(instructions, p[1])
        p[0] = instructions

    def p_staticarguments(self, p):
        '''staticarguments : COMMA staticvalue staticarguments
                           | empty'''
        p[0] = []
        if len(p) > 2:
            p[0].append(["AttributeFactory", [None, None, p[2]]])
            p[0].extend(p[3])

    def p_arguments(self, p):
        '''arguments : argument
                     | argument COMMA arguments'''
        p[0] = [p[1]]
        if len(p) > 2:
            p[0].extend(p[3])

    def p_argument(self, p):
        '''argument : staticvalue
                    | NAME DOT NAME
                    | NAME LPAR argument staticarguments RPAR'''
        case = len(p) - 1
        if case == 1:
            p[0] = ["AttributeFactory", [None, None, p[1]]]
        elif case == 3:
            p[0] = ["AttributeFactory", [None, p[3], None]]
        elif case == 5:
            p[0] = [p[1], [p[3], *p[4]]]

    def p_mixedarguments(self, p):
        '''mixedarguments : argument
                          | attribute
                          | argument COMMA mixedarguments
                          | attribute COMMA mixedarguments'''
        p[0] = [p[1]]
        if len(p) > 2:
            p[0].extend(p[3])

    def p_empty(self, p):
        '''empty : '''

    def p_error(self, t):
        token_underlined = '\u0332'.join(t.value)
        raise SchemaConfigException(f"Couldn't resolve token '{t.value}' at position {t.lexpos}\nCONTEXT: \n...{t.lexer.lexdata[max(0, t.lexpos-20):t.lexpos]}{token_underlined}\n...")

    @staticmethod
    def _cleanup_string(value):
        """
        Removes parentheses from string if value is of instance string.
        This is a required hack as the direct regex rule doesn't work here (don't know why).
        """
        if isinstance(value, str):  # hack to remove quotes, direct match didnt work
            return value[1:-1]
        return value


def _precompile(input_string):
    # Remove all comments
    match = re.search("#.*", input_string)
    while match is not None:
        input_string = input_string[:match.start()] + \
            input_string[match.end():]
        match = re.search("#.*", input_string)
    return input_string


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
            unraveled.append(_compile_instructions(arg))
        else:
            unraveled.append(arg)
    return unraveled


def _compile_module(module: str, args: List) -> "Factory":
    """Compiles a module and args into a factory"""
    factory_args = _unravel_args(args)
    logger.debug(f"Compiling module '{module}' with static args {[arg for arg in factory_args if isinstance(arg, str)]}")
    return get_factory(module)(*factory_args)


def _compile_instructions(instructions: List) -> "Factory":
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


def compile_schema(schema: str) -> List["Factory"]:
    """Parses a config schema file into usable factories.

    Args:
        schema: The schema as a string.
    Returns:
        A tuple (compiled_factory_dict, node_mask, relationship_mask)
        compiled_factory_dict: A dict in form of (entity_type_name, (NodeSupplyChain, RelationshipSupplyChain))
        for all provided entity_types.
        node_mask: A set of all entities that produce a node.
        relationship_mask: A set of all entities that produce a relationship.
    """
    # Removes comments
    precompiled_string = _precompile(schema)

    parser = SchemaConfigParser()
    instructions = parser.parse(precompiled_string)
    compiled = {}
    relationship_mask = set()
    node_mask = set()

    for entity_type, entity_instructions in instructions:
        if entity_type in compiled.keys():
            raise SchemaConfigException(f"Found two conflicting definitions of entity '{entity_type}'. Please only specify each entity once.")
        node_instructions, relationship_instructions = entity_instructions
        node_factories, relationship_factories = _compile_instructions(node_instructions), _compile_instructions(relationship_instructions)
        compiled[entity_type] = (get_factory("SupplyChain")(node_factories, "NodeSupplyChain"),
                                 get_factory("SupplyChain")(relationship_factories, "RelationSupplyChain"))
        if len(node_factories) > 0:
            node_mask.add(entity_type)
        if len(relationship_factories) > 0:
            relationship_mask.add(entity_type)
    return compiled, node_mask, relationship_mask
