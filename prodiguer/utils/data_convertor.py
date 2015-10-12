# -*- coding: utf-8 -*-

"""
.. module:: pyesdoc.utils.data_convertor.py
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

from prodiguer.utils.string_convertor import to_camel_case



# Set of types to be ignored when jsonifying.
_IGNOREABLE = (int, float, long, type(None), unicode)

# Set of unicodeable types used in jsonifying.
_UNICODEABLE = (basestring, datetime.datetime, uuid.UUID)


def convert(data, key_convertor=None):
    """Converts input data to a dictionary.

    :param object data: Data to be converted.

    :returns: Converted data.
    :rtype: object

    """
    if isinstance(data, _IGNOREABLE):
        return data

    elif isinstance(data, _UNICODEABLE):
        return unicode(data)

    elif isinstance(data, collections.Mapping):
        return {k if key_convertor is None else key_convertor(k):
                convert(v, key_convertor) for k, v in data.iteritems()}

    elif isinstance(data, collections.Iterable):
        return [convert(i, key_convertor) for i in data]

    elif sa.inspect(data, False):
        return  convert({c.name: getattr(data, c.name)
                         for c in sa.inspect(data).mapper.columns}, key_convertor)

    else:
        return data


def jsonify(data):
    """Converts input dictionary to json.

    :param dict data: Data in dictionary format.
    :param bool sort_keys: Flag indicating whether the dictionary keys will be sorted.

    :returns: JSON encoded string.
    :rtype: str

    """
    return json.dumps(convert(data, key_convertor=to_camel_case))
