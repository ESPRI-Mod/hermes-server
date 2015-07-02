# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.web.sim_metrics._validator.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation metric API request validators.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import re

import voluptuous

from prodiguer.db.mongo import dao_metrics as dao
from prodiguer.web import utils_handler
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


def _validate_group_query_arg(handler):
    """Validates fetch by group query arguments.

    """
    schema = voluptuous.Schema({
        voluptuous.Required(_PARAM_GROUP): voluptuous.All(list, _Sequence(str), _GroupID())
    })
    schema(handler.request.query_arguments)


def validate_add(handler):
    """Validates add POST endpoint HTTP request.

    """
    # if self.request.body:
    #     utils.validate_http_content_type(self, _CONTENT_TYPE_JSON)
    utils_handler.validate_request(
        handler, query_validator=_validate_group_query_arg)


def validate_delete(handler):
    """Validates delete POST endpoint HTTP request.

    """
    # if self.request.body:
    #     utils.validate_http_content_type(self, _CONTENT_TYPE_JSON)
    utils_handler.validate_request(
        handler, query_validator=_validate_group_query_arg)


def validate_fetch(handler):
    """Validates fetch GET endpoint HTTP request.

    """
    utils_handler.validate_request(
        handler, query_validator=_validate_group_query_arg)


def validate_fetch_columns(handler):
    """Validates fetch columns GET endpoint HTTP request.

    """
    utils_handler.validate_request(
        handler, query_validator=_validate_group_query_arg)


def validate_fetch_count(handler):
    """Validates fetch count GET endpoint HTTP request.

    """
    utils_handler.validate_request(
        handler, query_validator=_validate_group_query_arg)


def validate_fetch_setup(handler):
    """Validates fetch setup GET endpoint HTTP request.

    """
    # if self.request.body:
    #     utils.validate_http_content_type(self, _CONTENT_TYPE_JSON)
    utils_handler.validate_request(
        handler, query_validator=_validate_group_query_arg)


def validate_rename(handler):
    """Validates rename POST endpoint HTTP request.

    """
    # if self.request.body:
    #     utils.validate_http_content_type(self, _CONTENT_TYPE_JSON)
    utils_handler.validate_request(
        handler, query_validator=_validate_group_query_arg)


def validate_set_hashes(handler):
    """Validates set hashes POST endpoint HTTP request.

    """
    utils_handler.validate_request(
        handler, query_validator=_validate_group_query_arg)
