# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.api.handlers.metric.utils.py
   :copyright: Copyright "Feb 7, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation metric utility functions.

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>


"""
import json, re

from .... utils import config, convert
from .... db.mongo import dao_metrics as dao



# Regular expression for validating group name.
_GROUP_NAME_REGEX = '[^a-zA-Z0-9_-]'

# Min/max length of group name.
_GROUP_NAME_MIN_LENGTH = 4
_GROUP_NAME_MAX_LENGTH = 256

# HTTP header - Content-Type.
_HTTP_HEADER_CONTENT_TYPE = "Content-Type"

# HTTP header names.
_HTTP_HEADER_ACCESS_CONTROL_ALLOW_ORIGIN = "Access-Control-Allow-Origin"

# Inclide db id query parameter name.
_PARAM_INCLUDE_DB_ID = 'include_db_id'


def set_cors_white_list(handler):
    """Sets CORS whilte list from configuration.

    :param tornado.web.RequestHandler handler: A web request handler.

    """
    handler.set_header(_HTTP_HEADER_ACCESS_CONTROL_ALLOW_ORIGIN, "*")
    # handler.set_header(_HTTP_HEADER_ACCESS_CONTROL_ALLOW_ORIGIN,
    #                    ",".join(config.api.metric.cors_white_list))


def validate_group_name(group, validate_db_collection=True):
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


def decode_json_payload(handler, as_namedtuple=True):
    """Decode request body.

    :param tornado.web.RequestHandler handler: A web request handler.

    :returns: Decoded json data.
    :rtype: namedtuple | None

    """
    payload = json.loads(handler.request.body)
    if as_namedtuple:
        payload = convert.dict_to_namedtuple(payload)

    return payload


def validate_payload(payload, config):
    """Validate request payload.

    :param namedtuple payload: Payload being validated.
    :param list config: Validation configuration.

    """
    for fname, ftype in config:
        if fname not in payload._fields:
            raise KeyError("Undefined field: {0}".format(fname))
        if not isinstance(getattr(payload, fname), ftype):
            raise ValueError("Invalid field type: {0}".format(fname))


def validate_http_content_type(handler, expected_type):
    """Validates HTTP Content-Type request header."""
    if _HTTP_HEADER_CONTENT_TYPE not in handler.request.headers:
        raise ValueError("Content-Type is undefined")
    if handler.request.headers[_HTTP_HEADER_CONTENT_TYPE] != expected_type:
        raise ValueError("Unsupported content-type")


def validate_include_db_id(handler):
    """Validates the include_db_id query parameter.

    :param tornado.web.RequestHandler handler: A web request handler.

    """
    if _PARAM_INCLUDE_DB_ID in handler.request.arguments:
        if handler.get_argument(_PARAM_INCLUDE_DB_ID) not in ('true', 'false'):
            raise ValueError("Invalid request parameter {0}".format(_PARAM_INCLUDE_DB_ID))


def decode_include_db_id(handler):
    """Decodes the include_db_id query parameter.

    :param tornado.web.RequestHandler handler: A web request handler.

    :returns: Query parameter value if specified otherwise True.
    :rtype: bool

    """
    if _PARAM_INCLUDE_DB_ID not in handler.request.arguments or \
       handler.get_argument(_PARAM_INCLUDE_DB_ID) == 'false':
        return False
    else:
        return True
