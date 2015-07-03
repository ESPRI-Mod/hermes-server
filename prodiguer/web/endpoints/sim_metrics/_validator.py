# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.web.endpoints.sim_metrics._validator.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation metric API request validators.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import re

from voluptuous import All, Required, Schema

from prodiguer.db.mongo import dao_metrics as dao
from prodiguer.web.utils.validation import Sequence
from prodiguer.web.utils.validation import validate_request
from prodiguer.web.endpoints.sim_metrics import utils



# Regular expression for validating group name.
_GROUP_NAME_REGEX = '[^a-zA-Z0-9_-]'

# Min/max length of group name.
_GROUP_NAME_MIN_LENGTH = 4
_GROUP_NAME_MAX_LENGTH = 256

# Query parameter names.
_PARAM_GROUP = 'group'


def _GroupID():
    """Validates incoming group-id query parameter.

    """
    def f(val):
        """Inner function.

        """
        group = val[0]
        if re.compile(_GROUP_NAME_REGEX).search(group) or \
           ( len(group) < _GROUP_NAME_MIN_LENGTH or
             len(group) > _GROUP_NAME_MAX_LENGTH ) :
            raise ValueError("Invalid metric group name: {0}".format(group))
        if not dao.exists(group):
            raise ValueError("{0} db collection not found".format(group))

    return f


def _validate_group_query_arg(handler):
    """Validates fetch by group query arguments.

    """
    schema = Schema({
        Required(_PARAM_GROUP): All(list, Sequence(str), _GroupID())
    })
    schema(handler.request.query_arguments)


def validate_add(handler):
    """Validates add endpoint HTTP request.

    """
    # if self.request.body:
    #     utils.validate_http_content_type(self, _CONTENT_TYPE_JSON)
    validate_request(handler, query_validator=_validate_group_query_arg)


def validate_delete(handler):
    """Validates delete endpoint HTTP request.

    """
    # if self.request.body:
    #     utils.validate_http_content_type(self, _CONTENT_TYPE_JSON)
    validate_request(handler, query_validator=_validate_group_query_arg)


def validate_fetch(handler):
    """Validates fetch endpoint HTTP request.

    """
    validate_request(handler, query_validator=_validate_group_query_arg)


def validate_fetch_columns(handler):
    """Validates fetch_columns endpoint HTTP request.

    """
    validate_request(handler, query_validator=_validate_group_query_arg)


def validate_fetch_count(handler):
    """Validates fetch_count endpoint HTTP request.

    """
    validate_request(handler, query_validator=_validate_group_query_arg)


def validate_fetch_list(handler):
    """Validates fetch_list endpoint HTTP request.

    """
    validate_request(handler)


def validate_fetch_setup(handler):
    """Validates fetch_setup endpoint HTTP request.

    """
    # if self.request.body:
    #     utils.validate_http_content_type(self, _CONTENT_TYPE_JSON)
    validate_request(handler, query_validator=_validate_group_query_arg)


def validate_rename(handler):
    """Validates rename endpoint HTTP request.

    """
    # if self.request.body:
    #     utils.validate_http_content_type(self, _CONTENT_TYPE_JSON)
    validate_request(handler, query_validator=_validate_group_query_arg)


def validate_set_hashes(handler):
    """Validates set_hashes endpoint HTTP request.

    """
    validate_request(handler, query_validator=_validate_group_query_arg)
