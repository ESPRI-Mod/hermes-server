# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.web.endpoints.sim_metrics.request_validator.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Validates simulation metrics endpoint requests.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import re

from voluptuous import All, Required, Schema

from prodiguer.db.mongo import dao_metrics as dao
from prodiguer.web.request_validation import validator as rv



# Regular expression for validating group name.
_GROUP_NAME_REGEX = '[^a-zA-Z0-9_-]'

# Min/max length of group name.
_GROUP_NAME_MIN_LENGTH = 4
_GROUP_NAME_MAX_LENGTH = 256

# Query parameter names.
_PARAM_COLUMNS = 'columns'
_PARAM_DUPLICATE_ACTION = 'duplicate_action'
_PARAM_GROUP = 'group'
_PARAM_METRICS = 'metrics'
_PARAM_NEW_NAME = 'new_name'

# Set of allowed duplicate actions.
_DUPLICATE_ACTIONS = {u'skip', u'force'}


def _GroupName(assert_exists):
    """Validates group name.

    """
    def f(val):
        """Inner function.

        """
        # Validate reg-ex.
        if re.compile(_GROUP_NAME_REGEX).search(val):
            raise ValueError("Metric group name contains invalid characters: {0}".format(val))

        # Validate length.
        if len(val) < _GROUP_NAME_MIN_LENGTH or \
           len(val) > _GROUP_NAME_MAX_LENGTH:
            raise ValueError("Metric group name length is out of bounds: {0}".format(val))

        # Validate exists in db.
        if assert_exists and not dao.exists(val):
            raise ValueError("{0} db collection not found".format(val))

    return f


def _DuplicateAction():
    """Validates incoming duplicate_action query parameter.

    """
    def f(val):
        """Inner function.

        """
        if val not in _DUPLICATE_ACTIONS:
            raise ValueError("Invalid duplicate action: {}".format(val))

    return f


def _Metrics(body):
    """Validates a set of metrics.

    """
    def f(val):
        """Inner function.

        """
        # Validate metrics count > 0.
        if len(val) == 0:
            raise ValueError("No metrics to add")

        # Validate that length of each metric is same as length of group columns.
        for metric in val:
            if len(metric) != len(body['columns']):
                raise ValueError("Number of values does not match number of columns")


    return f


def validate_add(handler):
    """Validates add endpoint HTTP request.

    """
    def _validate_query():
        """Validates HTTP request query arguments.

        """
        rv.validate_data(handler.request.query_arguments, {
            _PARAM_DUPLICATE_ACTION: All(rv.Sequence(unicode), _DuplicateAction())
            })


    def _validate_body():
        """Validates HTTP request body.

        """
        body = handler.decode_json_body(False)

        print "AAAAAAA",'status' in body

        rv.validate_data(body, {
            Required(_PARAM_GROUP): All(unicode, _GroupName(False)),
            Required(_PARAM_COLUMNS): All(rv.Sequence(unicode, 0)),
            Required(_PARAM_METRICS): All(rv.Sequence(list, 0), _Metrics(body))
            })


    rv.validate(handler, body_validator=_validate_body, query_validator=_validate_query)


def validate_delete(handler):
    """Validates delete endpoint HTTP request.

    """
    def _validate_query():
        """Validates HTTP request query arguments.

        """
        rv.validate_data(handler.request.query_arguments, {
            Required(_PARAM_GROUP): All(rv.Sequence(unicode), _GroupName(True))
        })

    rv.validate(handler, query_validator=_validate_query)


def validate_fetch(handler):
    """Validates fetch endpoint HTTP request.

    """
    def _validate_query():
        """Validates HTTP request query arguments.

        """
        rv.validate_data(handler.request.query_arguments, {
            Required(_PARAM_GROUP): All(rv.Sequence(unicode), _GroupName(True))
        })

    rv.validate(handler, query_validator=_validate_query)


def validate_fetch_columns(handler):
    """Validates fetch_columns endpoint HTTP request.

    """
    def _validate_query():
        """Validates HTTP request query arguments.

        """
        rv.validate_data(handler.request.query_arguments, {
            Required(_PARAM_GROUP): All(rv.Sequence(unicode), _GroupName(True))
        })

    rv.validate(handler, query_validator=_validate_query)


def validate_fetch_count(handler):
    """Validates fetch_count endpoint HTTP request.

    """
    def _validate_query():
        """Validates HTTP request query arguments.

        """
        rv.validate_data(handler.request.query_arguments, {
            Required(_PARAM_GROUP): All(rv.Sequence(unicode), _GroupName(True))
        })

    rv.validate(handler, query_validator=_validate_query)


def validate_fetch_list(handler):
    """Validates fetch_list endpoint HTTP request.

    """
    rv.validate(handler)


def validate_fetch_setup(handler):
    """Validates fetch_setup endpoint HTTP request.

    """
    def _validate_query():
        """Validates HTTP request query arguments.

        """
        rv.validate_data(handler.request.query_arguments, {
            Required(_PARAM_GROUP): All(rv.Sequence(unicode), _GroupName(True))
        })

    rv.validate(handler, query_validator=_validate_query)


def validate_rename(handler):
    """Validates rename endpoint HTTP request.

    """
    def _validate_query():
        """Validates HTTP request query arguments.

        """
        rv.validate_data(handler.request.query_arguments, {
            Required(_PARAM_GROUP): All(rv.Sequence(unicode), _GroupName(True)),
            Required(_PARAM_NEW_NAME): All(rv.Sequence(unicode), _GroupName(False))
        })

    rv.validate(handler, query_validator=_validate_query)


def validate_set_hashes(handler):
    """Validates set_hashes endpoint HTTP request.

    """
    def _validate_query():
        """Validates HTTP request query arguments.

        """
        rv.validate_data(handler.request.query_arguments, {
            Required(_PARAM_GROUP): All(rv.Sequence(unicode), _GroupName(True))
        })

    rv.validate(handler, query_validator=_validate_query)
