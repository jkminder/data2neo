#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2021, Nigel Small
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
This module provides facilities for encoding values into Cypher
identifiers and literals. Adapted and copied from the now EOL py2neo: https://github.com/py2neo-org/py2neo

authors: Julian Minder and Nigel Small
"""

from re import compile as re_compile
from unicodedata import category

ID_START = {u"_"} | {chr(x) for x in range(0xFFFF)
                     if category(chr(x)) in ("LC", "Ll", "Lm", "Lo", "Lt", "Lu", "Nl")}
ID_CONTINUE = ID_START | {chr(x) for x in range(0xFFFF)
                          if category(chr(x)) in ("Mn", "Mc", "Nd", "Pc", "Sc")}

DOUBLE_QUOTE = u'"'
SINGLE_QUOTE = u"'"

ESCAPED_DOUBLE_QUOTE = u'\\"'
ESCAPED_SINGLE_QUOTE = u"\\'"

X_ESCAPE = re_compile(r"(\\x([0-9a-f]{2}))")
DOUBLE_QUOTED_SAFE = re_compile(r"([ -!#-\[\]-~]+)")
SINGLE_QUOTED_SAFE = re_compile(r"([ -&(-\[\]-~]+)")

atomic_types = (bool, bytearray, bytes, float, int, str)
bytes_types = (bytearray, bytes)
integer_types = (int,)
list_types = (list, map)
numeric_types = (int, float)
string_types = (bytes, str)
unicode_types = (str,)

encoding = "utf-8"
sequence_separator = u", "
key_value_separator = u": "
def xstr(s, encoding="utf-8"):
    """ Convert argument to string type returned by __str__.
    """
    if isinstance(s, str):
        return s
    elif isinstance(s, bytes):
        return s.decode(encoding)
    else:
        return str(s)

ustr = lambda s: xstr(s, encoding)

def is_safe_key(key):
    key = ustr(key)
    return key[0] in ID_START and all(key[i] in ID_CONTINUE for i in range(1, len(key)))


class CypherExpression:
    """Dummy class for wrapping Cypher expressions such that they are not escaped."""
    def __init__(self, value):
        self.__value = value

    @property
    def value(self):
        return self.__value


def encode_key(key):
    key = ustr(key)
    if not key:
        raise ValueError("Keys cannot be empty")
    if is_safe_key(key):
        return key
    else:
        return u"`" + key.replace(u"`", u"``") + u"`"

def encode_string(value):
    value = ustr(value)

    num_single = value.count(u"'")
    num_double = value.count(u'"')
    quote = SINGLE_QUOTE if num_single <= num_double else DOUBLE_QUOTE

    if quote == SINGLE_QUOTE:
        escaped_quote = ESCAPED_SINGLE_QUOTE
        safe = SINGLE_QUOTED_SAFE
    else: # quote == DOUBLE_QUOTE:
        escaped_quote = ESCAPED_DOUBLE_QUOTE
        safe = DOUBLE_QUOTED_SAFE


    if not value:
        return quote + quote

    parts = safe.split(value)
    for i in range(0, len(parts), 2):
        parts[i] = (X_ESCAPE.sub(u"\\\\u00\\2", parts[i].encode("unicode-escape").decode("utf-8")).
                    replace(quote, escaped_quote).replace(u"\\u0008", u"\\b").replace(u"\\u000c", u"\\f"))
    return quote + u"".join(parts) + quote

def encode_list(values):
        return u"[" + sequence_separator.join(map(encode_value, values)) + u"]"

def encode_map(values):
    return u"{" + sequence_separator.join(encode_key(key) + key_value_separator +
                                                encode_value(value) for key, value in values.items()) + u"}"

def encode_value(value):
        if value is None:
            return u"null"
        if value is True:
            return u"true"
        if value is False:
            return u"false"
        if isinstance(value, CypherExpression):
            return value.value
        if isinstance(value, numeric_types):
            return ustr(value)
        if isinstance(value, string_types):
            return encode_string(value)
        if isinstance(value, list):
            return encode_list(value)
        if isinstance(value, dict):
            return encode_map(value)
        raise TypeError("Cypher literal values of type %s.%s are not supported" %
                        (type(value).__module__, type(value).__name__))

