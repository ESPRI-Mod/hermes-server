# -*- coding: utf-8 -*-

"""
.. module:: pyesdoc.utils.convertor.py
   :copyright: Copyright "Feb 7, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Set of conversion functions.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import re



# Values considered to be abbreviations.
_ABBREVIATIONS = ("id", "uid", "uuid")

# Default separator.
_DEFAULT_SEPARATOR = "_"


def to_pascal_case(target, separator=_DEFAULT_SEPARATOR):
    """Converts passed name to pascal case.

    :param target: A string to be converted.
    :type target: str

    :param separator: A separator used to split target string into constituent parts.
    :type separator: str

    :returns: The target string converted to pascal case.
    :rtype: str

    """
    r = ''
    if target is not None and len(target):
        # Preserve initial separator
        if target[0:len(separator)] == separator:
            r = separator

        # Iterate string parts.
        s = target.split(separator)
        for s in s:

            # Upper case abbreviations.
            if s.lower() in _ABBREVIATIONS:
                r += s.upper()

            # Upper case initial character.
            elif (len(s) > 0):
                r += s[0].upper()
                if (len(s) > 1):
                    r += s[1:]

    return r


def to_camel_case(target, separator=_DEFAULT_SEPARATOR):
    """Converts passed name to camel case.

    :param target: A string to be converted.
    :type target: str

    :param separator: A separator used to split target string into constituent parts.
    :type separator: str

    :returns: The target string converted to camel case.
    :rtype: str

    """
    r = ''
    if target is not None and len(target):
        # Convert to pascal case.
        s = to_pascal_case(target, separator)

        # Preserve initial separator
        if s[0:len(separator)] == separator:
            r += separator
            s = s[len(separator):]

        # Lower case abbreviations.
        if s.lower() in _ABBREVIATIONS:
            r += s.lower()

        # Lower case initial character.
        elif len(s):
            r += s[0].lower()
            r += s[1:]

    return r


def to_underscore_case(target):
    """Helper function to convert a from camel case string to an underscore case string.

    :param target: A string for conversion.
    :type target: str

    :returns: A string converted to underscore case, e.g. account_number.
    :rtype: str

    """
    if target is None or not len(target):
        return ''

    r = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', target)
    r = re.sub('([a-z0-9])([A-Z])', r'\1_\2', r)
    r = r.lower()

    return r


def to_spaced_case(target, separator=_DEFAULT_SEPARATOR):
    """Helper function to convert a string value from camel case to spaced case.

    :param target: A string for conversion.
    :type target: str

    :returns: A string converted to spaced case.
    :rtype: str

    """
    if target is None:
        return ""
    elif separator is not None and len(target.split(separator)) > 1:
        return " ".join(target.split(separator))
    elif target.find(" ") == -1:
        return re.sub("([A-Z])"," \g<0>", target).strip()
    else:
        return target
