# -*- coding: utf-8 -*-

"""
.. module:: pyesdoc.utils.convertor.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Set of conversion functions.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import collections
import datetime
import json
import uuid

import sqlalchemy as sa
from bson import json_util

from prodiguer.utils.string_convertor import to_camel_case
from prodiguer.utils import config



# Set of types to be ignored when jsonifying.
_JSONIFYING_IGNOREABLE = (int, float, long, type(None), unicode)

# Set of unicodeable types used in jsonifying.
_JSONIFYING_UNICODEABLE = (basestring, datetime.datetime, uuid.UUID)


def _props(data):
    """Returns set of public class instance properties.

    """
    return {k for k in dir(data) if k not in dir(data.__class__)}


def _format_key(key):
    """Formats a dictionary key.

    """
    return unicode(to_camel_case(key))


def _jsonify_entity(data):
    """Converts a SqlAlchemy mapped entity to a dictionary.

    """
    cols = sa.inspect(data).mapper.columns

    return _jsonify({ c.name: getattr(data, c.name) for c in cols })


def _jsonify(data):
    """Prepares data for json encoding.

    """
    if isinstance(data, _JSONIFYING_IGNOREABLE):
        return data

    elif isinstance(data, _JSONIFYING_UNICODEABLE):
        return unicode(data)

    elif isinstance(data, collections.Mapping):
        return {_format_key(k): _jsonify(v) for k, v in data.iteritems()}

    elif isinstance(data, collections.Iterable):
        return [_jsonify(i) for i in data]

    elif sa.inspect(data, False):
        return _jsonify_entity(data)

    else:
        return data


def jsonify(data):
    """Converts input dictionary to json.

    :param dict data: Data in dictionary format.

    :returns: JSON encoded string.
    :rtype: str

    """
    return json.dumps(_jsonify(data),
                      default=json_util.default,
                      indent=4 if config.data.deploymentMode == 'dev' else None,
                      sort_keys=True)
