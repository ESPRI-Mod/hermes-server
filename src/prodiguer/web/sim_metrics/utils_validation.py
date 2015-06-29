# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.web.sim_metrics.utils.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation metric utility functions.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import re

import voluptuous

from prodiguer.db.mongo import dao_metrics as dao
from prodiguer.web.sim_metrics import utils



# Regular expression for validating group name.
_GROUP_NAME_REGEX = '[^a-zA-Z0-9_-]'

# Min/max length of group name.
_GROUP_NAME_MIN_LENGTH = 4
_GROUP_NAME_MAX_LENGTH = 256

# Query parameter names.
_PARAM_GROUP = 'group'


def _validate_group_name(group, validate_db_collection=True):
    """Validates a simulation metric group name.

    :param str group: A simulation metric group name.

    """
    def throw():
        raise ValueError("Invalid metric group name: {0}".format(group))

    if re.compile(_GROUP_NAME_REGEX).search(group):
        throw()
    if len(group) < _GROUP_NAME_MIN_LENGTH or \
       len(group) > _GROUP_NAME_MAX_LENGTH:
        throw()
    if validate_db_collection and not dao.exists(group):
        raise ValueError("{0} db collection not found".format(group))


def _GroupID():
    """Validates incoming group-id query parameter.

    """
    def f(val):
        """Inner function.

        """
        _validate_group_name(val[0])

    return f


def _Sequence(expected_type, expected_length=1):
    """Validates a sequence of query parameter values.

    """
    def f(val):
        """Inner function.

        """
        # Validate sequence length.
        if len(val) != expected_length:
            raise ValueError("Invalid request")

        # Validate sequence type.
        for item in val:
            try:
                expected_type(item)
            except ValueError:
                raise ValueError("Invalid request")

        return val

    return f


def validate_fetch_query_arguments(handler):
    """Validates fetch GET endpoint HTTP query arguments.

    """
    schema = voluptuous.Schema({
        voluptuous.Required(_PARAM_GROUP): voluptuous.All(list, _Sequence(str), _GroupID())
    })
    schema(handler.request.query_arguments)


def validate_fetch_columns_query_arguments(handler):
    """Validates fetch columns GET endpoint HTTP query arguments.

    """
    schema = voluptuous.Schema({
        voluptuous.Required(_PARAM_GROUP): voluptuous.All(list, _Sequence(str), _GroupID())
    })
    schema(handler.request.query_arguments)


def validate_fetch_count_query_arguments(handler):
    """Validates fetch count GET endpoint HTTP query arguments.

    """
    schema = voluptuous.Schema({
        voluptuous.Required(_PARAM_GROUP): voluptuous.All(list, _Sequence(str), _GroupID())
    })
    schema(handler.request.query_arguments)


def validate_fetch_setup_query_arguments(handler):
    """Validates fetch setup GET endpoint HTTP query arguments.

    """
    schema = voluptuous.Schema({
        voluptuous.Required(_PARAM_GROUP): voluptuous.All(list, _Sequence(str), _GroupID())
    })
    schema(handler.request.query_arguments)
