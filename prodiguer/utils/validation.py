# -*- coding: utf-8 -*-

"""
.. module:: hermes.utils.validation.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Variable validation utilities.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>

"""
import uuid

import arrow



def _raise_value_error(val, var, var_type):
    """Raises a generic value error.

    """
    raise ValueError('{} [{}] is an invalid {}'.format(var, val, var_type))


def validate_entity_type(etype):
    """Validates a type to ensure that it is a recognized entity.

    :param class etype: A supported entity type.

    """
    from prodiguer.db.pgres.entity import Entity

    if not issubclass(etype, Entity):
        raise TypeError('DB entity type is unknown :: {}'.format(etype))


def validate_bool(val, var):
    """Validates a boolean.

    """
    if val is None:
        raise ValueError('{} is undefined bool'.format(var))

    try:
        bool(val)
    except ValueError:
        _raise_value_error(val, var, bool)


def validate_int(val, var):
    """Validates an integer.

    """
    if val is None:
        raise ValueError('{} is undefined'.format(var))

    try:
        int(val)
    except ValueError:
        _raise_value_error(val, var, int)


def validate_float(val, var):
    """Validates a flaot.

    """
    if val is None:
        raise ValueError('{} is undefined'.format(var))

    try:
        float(val)
    except ValueError:
        _raise_value_error(val, var, float)


def validate_date(val, var, date_format=None):
    """Validates a date.

    """
    if val is None:
        raise ValueError('{} is undefined date'.format(var))

    try:
        if date_format is not None:
            arrow.get(val, date_format)
        else:
            arrow.get(val)
    except arrow.parser.ParserError:
        _raise_value_error(val, var, 'date')


def validate_str(val, var):
    """Validates a string.

    """
    if val is None:
        raise ValueError('{} is undefined string'.format(var))

    try:
        val = str(val)
    except ValueError:
        _raise_value_error(val, var, str)

    if not len(val):
        raise ValueError('{} is empty string'.format(var))


def validate_uid(val, var):
    """Validaes a universally unique identifier.

    """
    if not isinstance(val, uuid.UUID):
        try:
            uuid.UUID(val)
        except ValueError:
            _raise_value_error(val, var, uuid.UUID)


def validate_ucode(val, var):
    """Validates a unicode.

    """
    if val is None:
        raise ValueError('{} is undefined unicode'.format(var))

    try:
        val = unicode(val)
    except ValueError:
        _raise_value_error(val, var, unicode)

    if not len(val):
        raise ValueError('{} is empty unicode'.format(var))


def validate_iterable(val, var):
    """Validates an iterable.

    """
    if val is None:
        raise ValueError('{} is undefined iterable'.format(var))

    try:
        iter(val)
    except TypeError:
        _raise_value_error(val, var, iter)


def validate_mbr(item, collection, var, convertor=None):
    """Validates a collection member.

    """
    if convertor:
        try:
            item = convertor(item)
        except ValueError:
            raise ValueError('{} is undefined member of {}'.format(item, var))
    if item not in collection:
        raise ValueError('{} is undefined member of {}'.format(item, var))


def validate_vrs(version, var):
    """Validates a version identifier.

    :param str version: A version identifier.

    """
    try:
        elements = version.split('.')
    except AttributeError:
        raise ValueError("{} must consist of '.' delimited integers : {}".format(var, version))
    try:
        [int(v) for v in elements]
    except ValueError:
        raise ValueError("{} must consist of '.' delimited integers : {}".format(var, version))
