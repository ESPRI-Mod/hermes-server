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
from prodiguer.web.utils.request_validation import Sequence
from prodiguer.web.utils.request_validation import validate
from prodiguer.web.endpoints.sim_metrics import utils



# Regular expression for validating group name.
_GROUP_NAME_REGEX = '[^a-zA-Z0-9_-]'

# Min/max length of group name.
_GROUP_NAME_MIN_LENGTH = 4
_GROUP_NAME_MAX_LENGTH = 256

# Query parameter names.
_PARAM_GROUP = 'group'
_PARAM_DUPLICATE_ACTION = 'duplicate_action'
_PARAM_NEW_NAME = 'new_name'

# Set of expected add request body fields and their type.
_ADD_FIELDS = set([
    ('group', unicode),
    ('columns', list),
    ('metrics', list),
    ])


def _validate_group(group, assert_exists=False):
    """Validates a simulation metric group name.

    """
    if re.compile(_GROUP_NAME_REGEX).search(group) or \
       ( len(group) < _GROUP_NAME_MIN_LENGTH or
         len(group) > _GROUP_NAME_MAX_LENGTH ) :
        raise ValueError("Invalid metric group name: {0}".format(group))
    if assert_exists and not dao.exists(group):
        raise ValueError("{0} db collection not found".format(group))


def _GroupID(assert_exists=False):
    """Validates incoming group query parameter.

    """
    def f(val):
        """Inner function.

        """
        _validate_group(val[0], assert_exists)

    return f


def _DuplicateAction():
    """Validates incoming duplicate_action query parameter.

    """
    def f(val):
        """Inner function.

        """
        action = val[0]
        if action not in ['skip', 'force']:
            raise ValueError("Invalid duplicate action: {}".format(action))

    return f


def validate_add(handler):
    """Validates add endpoint HTTP request.

    """
    def _validate_query():
        """Validates HTTP request query arguments.

        """
        schema = Schema({
            _PARAM_DUPLICATE_ACTION: All(list, Sequence(str), _DuplicateAction())
        })
        schema(handler.request.query_arguments)


    def _validate_body():
        """Validates request body.

        """
        # Validate body.
        body = handler.decode_json_body()
        for fname, ftype in _ADD_FIELDS:
            if fname not in body._fields:
                raise KeyError("Undefined field: {0}".format(fname))
            if not isinstance(getattr(body, fname), ftype):
                raise ValueError("Invalid field type: {0}".format(fname))

        # Validate group name.
        _validate_group(body.group, False)

        # Validate metrics count > 0.
        if len(body.metrics) == 0:
            raise ValueError("No metrics to add")

        # Validate that length of each metric is same as length of group columns.
        for metric in body.metrics:
            if len(metric) != len(body.columns):
                raise ValueError("Invalid metric: number of values does not match number of columns")

    validate(
        handler,
        body_validator=_validate_body,
        query_validator=_validate_query)


def validate_delete(handler):
    """Validates delete endpoint HTTP request.

    """
    def _validate_query():
        """Validates HTTP request query arguments.

        """
        schema = Schema({
            Required(_PARAM_GROUP): All(list, Sequence(str), _GroupID(True))
        })
        schema(handler.request.query_arguments)

    validate(handler, query_validator=_validate_query)


def validate_fetch(handler):
    """Validates fetch endpoint HTTP request.

    """
    def _validate_query():
        """Validates HTTP request query arguments.

        """
        schema = Schema({
            Required(_PARAM_GROUP): All(list, Sequence(str), _GroupID(True))
        })
        schema(handler.request.query_arguments)

    validate(handler, query_validator=_validate_query)


def validate_fetch_columns(handler):
    """Validates fetch_columns endpoint HTTP request.

    """
    def _validate_query():
        """Validates HTTP request query arguments.

        """
        schema = Schema({
            Required(_PARAM_GROUP): All(list, Sequence(str), _GroupID(True))
        })
        schema(handler.request.query_arguments)

    validate(handler, query_validator=_validate_query)


def validate_fetch_count(handler):
    """Validates fetch_count endpoint HTTP request.

    """
    def _validate_query():
        """Validates HTTP request query arguments.

        """
        schema = Schema({
            Required(_PARAM_GROUP): All(list, Sequence(str), _GroupID(True))
        })
        schema(handler.request.query_arguments)

    validate(handler, query_validator=_validate_query)


def validate_fetch_list(handler):
    """Validates fetch_list endpoint HTTP request.

    """
    validate(handler)


def validate_fetch_setup(handler):
    """Validates fetch_setup endpoint HTTP request.

    """
    def _validate_query():
        """Validates HTTP request query arguments.

        """
        schema = Schema({
            Required(_PARAM_GROUP): All(list, Sequence(str), _GroupID(True))
        })
        schema(handler.request.query_arguments)

    validate(handler, query_validator=_validate_query)


def validate_rename(handler):
    """Validates rename endpoint HTTP request.

    """
    def _validate_query():
        """Validates HTTP request query arguments.

        """
        schema = Schema({
            Required(_PARAM_GROUP): All(list, Sequence(str), _GroupID(True)),
            Required(_PARAM_NEW_NAME): All(list, Sequence(str), _GroupID()),
        })
        schema(handler.request.query_arguments)

    validate(handler, query_validator=_validate_query)


def validate_set_hashes(handler):
    """Validates set_hashes endpoint HTTP request.

    """
    def _validate_query():
        """Validates HTTP request query arguments.

        """
        schema = Schema({
            Required(_PARAM_GROUP): All(list, Sequence(str), _GroupID(True))
        })
        schema(handler.request.query_arguments)

    validate(handler, query_validator=_validate_query)
