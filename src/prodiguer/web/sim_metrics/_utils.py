# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.web.sim_metrics.utils.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation metric utility functions.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import json
import re

from prodiguer.db.mongo import dao_metrics as dao
from prodiguer.utils import convert



# Regular expression for validating group name.
_GROUP_NAME_REGEX = '[^a-zA-Z0-9_-]'

# Min/max length of group name.
_GROUP_NAME_MIN_LENGTH = 4
_GROUP_NAME_MAX_LENGTH = 256

# HTTP header - Content-Type.
_HTTP_HEADER_CONTENT_TYPE = "Content-Type"

# HTTP CORS header.
_HTTP_HEADER_ACCESS_CONTROL_ALLOW_ORIGIN = "Access-Control-Allow-Origin"

# Metrics format query parameter name.
_PARAM_FORMAT = 'format'


def set_cors_white_list(handler):
    """Sets CORS whilte list from configuration.

    :param tornado.web.RequestHandler handler: A web request handler.

    """
    handler.set_header(_HTTP_HEADER_ACCESS_CONTROL_ALLOW_ORIGIN, "*")
    # TODO set white list handler.set_header(_HTTP_HEADER_ACCESS_CONTROL_ALLOW_ORIGIN,
    #                    ",".join(config.web.sim_metrics.cors_white_list))


def validate_group_name(group, validate_db_collection=True):
    """Validates a simulation metric group name.

    :param str group: A simulation metric group name.

    """
    def throw():
        raise ValueError("Invalid metric group name: {0}".format(group))

    if re.compile(_GROUP_NAME_REGEX).search(group):
        throw()
    print "SSS", len(group)
    if len(group) < _GROUP_NAME_MIN_LENGTH or \
       len(group) > _GROUP_NAME_MAX_LENGTH:
        throw()
    if validate_db_collection and not dao.exists(group):
        raise ValueError("{0} db collection not found".format(group))


def validate_duplicate_action(action):
    """Validates duplicate hash action.

    :param str action: Action to take when processing a metric set with a duplicate hash identifier.

    """
    if action not in ['skip', 'force']:
        raise ValueError("Invalid duplicate metric action")


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


def validate_http_content_type(handler, expected_types):
    """Validates HTTP Content-Type request header."""
    if _HTTP_HEADER_CONTENT_TYPE not in handler.request.headers:
        raise ValueError("Content-Type is undefined")
    header = handler.request.headers[_HTTP_HEADER_CONTENT_TYPE]
    if not header in expected_types:
        raise ValueError("Unsupported content-type")
