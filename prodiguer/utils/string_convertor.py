# -*- coding: utf-8 -*-

"""
.. module:: pyesdoc.utils.string_conversion.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Set of string conversion functions.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import re



# Values considered to be abbreviations.
_ABBREVIATIONS = ("id", "uid", "uuid")

# Default separator.
_DEFAULT_SEPARATOR = "_"


def to_pascal_case(target, separator=_DEFAULT_SEPARATOR):
    """Converts a string to pascal case.

    :param str target: A string to be converted.
    :param str separator: A separator used to split target string into parts.

    :returns: The target string converted to pascal case.
    :rtype: str

    """
    result = ''
    if target is not None and len(target):
        if target[0:len(separator)] == separator:
            result = separator
        for text in target.split(separator):
            if text.lower() in _ABBREVIATIONS:
                result += text.upper()
            elif (len(text) > 0):
                result += text[0].upper()
                if (len(text) > 1):
                    result += text[1:]
    return result


def to_camel_case(target, separator=_DEFAULT_SEPARATOR):
    """Converts a string to camel case.

    :param str target: A string to be converted.
    :param str separator: A separator used to split target string into parts.

    :returns: The target string converted to camel case.
    :rtype: str

    """
    result = ''
    if target is not None and len(target):
        text = to_pascal_case(target, separator)
        # Preserve initial separator
        if text[0:len(separator)] == separator:
            result += separator
            text = text[len(separator):]

        # Lower case abbreviations.
        if text.lower() in _ABBREVIATIONS:
            result += text.lower()

        # Lower case initial character.
        elif len(text):
            result += text[0].lower()
            result += text[1:]
    return result


def to_spaced_case(target, separator=_DEFAULT_SEPARATOR):
    """Converts a string to spaced case.

    :param str target: A string for conversion.

    :returns: A string converted to spaced case.
    :rtype: str

    """
    if target is None:
        return ""
    elif separator is not None and len(target.split(separator)) > 1:
        return " ".join(target.split(separator))
    elif target.find(" ") == -1:
        return re.sub("([A-Z])", r" \g<0>", target).strip()
    else:
        return target


def to_underscore_case(target):
    """Converts a string to underscore case.

    :param str target: A string for conversion.

    :returns: A string converted to underscore case, e.g. account_number.
    :rtype: str

    """
    if target is None or not len(target):
        return unicode()

    result = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', target)
    result = re.sub('([a-z0-9])([A-Z])', r'\1_\2', result)

    return result.lower()