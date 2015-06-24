# -*- coding: utf-8 -*-

"""
.. module:: utils.py

   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL / CeCILL
   :platform: Unix
   :synopsis: Set of test utility functions.

.. moduleauthor:: IPSL (ES-DOC) <dev@esdocumentation.org>

"""
import json
import datetime
import random
import uuid

import requests
from dateutil import parser as dateutil_parser

from prodiguer.db import pgres as db
from prodiguer.db.pgres.type_utils import Convertor



# API v1 base endpoint.
EP_BASE = r"http://localhost:8888/api/1"

# Heartbeat endpoint.
_EP_OPS_HEARTBEAT = EP_BASE + r"/ops/heartbeat"

# Http header: content types.
HTTP_CONTENT_TYPE_JSON = r"application/json; charset=utf-8"
HTTP_CONTENT_TYPE_CSV = r"application/csv; charset=utf-8"

# Http header: utf-8encoding.
HTTP_CONTENT_ENCODING_UTF8 = r'utf-8'

# Encoding types.
ENCODING_JSON = 'json'
ENCODING_CSV = 'csv'


def get_boolean():
    """Returns a random boolean for testing purposes.

    """
    return True


def get_date():
    """Returns a random date for testing purposes.

    """
    return datetime.datetime.now()


def get_datetime():
    """Returns a random datetime for testing purposes.

    """
    return datetime.datetime.now()


def get_int(lower=0, upper=9999999):
    """Returns a random integer for testing purposes.

    :param int lower: Lower limit for returned result.
    :param int upper: Upper limit for returned result.

    """
    return random.randint(lower, upper)


def get_long():
    """Returns a random long for testing purposes.

    """
    return long(get_int())


def get_float():
    """Returns a random float for testing purposes.

    """
    return random.random()


def get_string(len=63):
    """Returns a random string for testing purposes.

    :param int len: Limit for length of returned result.

    """
    return str(uuid.uuid1())[:len]


def get_unicode(len=63):
    """Returns a random unicde for testing purposes.

    :param int len: Limit for length of returned result.

    """
    return unicode(uuid.uuid1())[:len]


def get_uuid():
    """Returns a random uuid for testing purposes.

    """
    return uuid.uuid4()


def assert_nullable(actual, expected, assertor):
    """Asserts a nullable value.

    :param object actual: A value.
    :param object expected: Expected value.
    :param function assertor: Callback to execute if expected is a value.

    """
    if expected is None:
        assert_none(actual)
    else:
        assertor(actual, expected)


def assert_collection(collection, length = -1, types=None):
    """Asserts an object collection.

    :param list collection: An object collection.
    :param int length: Length of collection.
    :param types: Types that object must either be or sub-class from.
    :type types: list | class

    """
    assert_obj(collection)
    if length >= 0:
        assert len(collection) == length
    if len(collection) and types is not None:
        for instance in collection:
            assert_obj(instance, types)


def assert_none(instance):
    """Asserts an instance is none.

    :param object instance: An object for testing.

    """
    assert instance is None, \
           "Object is of type {0}.  Expected an object of type None.".format(instance.__class__)


def assert_obj(instance, types=None):
    """Asserts an object instance.

    :param object instance: An object for testing.
    :param types: Types that object must either be or sub-class from.
    :type types: list | class

    """
    assert instance is not None
    if types is not None:
        try:
            iter(types)
        except:
            types = [types]

        is_of_type = False
        for type in types:
            if isinstance(instance, type):
                is_of_type = True
                break
        assert is_of_type, "Object is of type {0}.  Expected an object of {1}.".format(instance.__class__, types)


def assert_object(instance, types=None):
    """Asserts an object instance.

    :param object instance: An object for testing.
    :param types: Types that object must either be or sub-class from.
    :type types: list | class

    """
    assert_obj(instance, types)


def assert_dict(instance, keys=None):
    """Asserts a dictionary.

    :param dict instance: A dictionary for testing.
    :param list keys: Expected dictionary keys.

    """
    assert_object(instance, dict)
    try:
        iter(keys)
    except TypeError:
        keys = ()
    missing = []
    for key in keys:
        if key not in instance:
            missing.append(key)
    assert len(missing) == 0, \
           "Dictionary keys not found: {0}".format(", ".join(missing))


def assert_namedtuple(instance, cls_name='_Class'):
    """Asserts a namedtuple.

    :param namedtuple instance: A namedtuple for testing.
    :param str cls_name: Expected name of namedtuple class.

    """
    assert_string(instance.__class__.__name__, cls_name)


def assert_string(actual, expected, startswith=False, ignore_case=False):
    """Asserts a string.

    :param str actual: A string.
    :param str expected: Expected string value.
    :param bool expected: Flag indicating whether to perform startswith test.
    :param bool ignore_case: Flag indicating whether to ignore case.

    """
    # Format.
    actual = actual.strip()
    expected = expected.strip()
    if ignore_case:
        actual = actual.lower()
        expected = expected.lower()

    # Assert.
    if startswith == False:
        assert actual == expected, \
               "Was {0}, expected {1}".format(actual, expected)
    else:
        assert actual.startswith(expected), \
               "{0} does not start with {1}".format(actual, expected)


def assert_bool(actual, expected=True):
    """Asserts a bool.

    :param bool actual: A boolean.
    :param bool expected: Expected boolean value.

    """
    assert actual == expected, \
           "Was {0}, expected {1}".format(actual, expected)


def assert_date(actual, expected):
    """Asserts a date.

    :param str actual: A date.
    :param str expected: Expected date value.

    """
    assert actual == dateutil_parser.parse(expected), \
           "Was {0}, expected {1}".format(actual, dateutil_parser.parse(expected))


def assert_datetime(actual, expected):
    """Asserts a datetime.

    :param str actual: A datetime.
    :param str expected: Expected datetime value.

    """
    assert actual == dateutil_parser.parse(expected), "Was {0}, expected {1}".format(actual, dateutil_parser.parse(expected))


def assert_integer(actual, expected):
    """Asserts an integer.

    :param int actual: An integer.
    :param int expected: Expected integer value.

    """
    assert actual == expected, "Was {0}, expected {1}".format(actual, expected)


def assert_uuid(actual, expected=None):
    """Asserts a uuid.

    :param str actual: A uuid.
    :param str expected: Expected uuid value.

    """
    if isinstance(actual, uuid.UUID) == False:
        actual = uuid.UUID(actual)
    if expected is None:
        return

    if isinstance(expected, uuid.UUID) == False:
        expected = uuid.UUID(expected)

    assert actual == expected, "Was {0}, expected {1}".format(actual, expected)


def assert_db_type_creation(type):
    """Performs set of entity creation tests.

    :param type: Type of entity being tested.
    :type type: python class

    """
    # Create instance directly.
    x = type()
    assert_obj(x, type)
    assert_obj(x, db.types.Entity)

    # Create instance via type factory.
    y = db.type_factory.create(type)
    assert_obj(y, type)

    assert_obj(y, db.types.Entity)

    # Reset type factory.
    db.type_factory.reset()


def assert_db_type_conversion(type):
    """Performs set of entity conversion tests.

    :param type: Type of entity being tested.
    :type type: python class

    """
    # Create instance via factory.
    x = db.type_factory.create(type)

    # Convert to string.
    assert_obj(Convertor.to_string(x), str)

    # Convert to dictionary.
    assert_obj(Convertor.to_dict(x), dict)

    # Convert to json.
    assert Convertor.to_json(x) is not None

    # Reset type factory.
    db.type_factory.reset()


def assert_db_type_persistence(type):
    """Performs set of entity persistence tests.

    :param type: Type of entity being tested.
    :type type: python class

    """
    # Cache current count.
    count = db.dao.get_count(type)

    # Create instance via factory.
    db.type_factory.create(type)

    # Reassert counts.
    new_count = db.dao.get_count(type)
    assert_integer(new_count, count + 1)

    # Delete & reassert count.
    db.type_factory.reset()
    new_count = db.dao.get_count(type)
    assert_integer(new_count, count)


def invoke_api(verb, url, payload=None):
    """Invokes api endpoint."""
    data = headers = None
    if payload:
        headers = {'content-type': 'application/json'}
        data = json.dumps(payload)

    return verb(url, data=data, headers=headers)


def assert_api_response(r,
                        expected_status,
                        expected_data=None,
                        expected_content_type=HTTP_CONTENT_TYPE_JSON,
                        expected_content_encoding=HTTP_CONTENT_ENCODING_UTF8,
                        response_data_parser=None):
    """Asserts an api response."""
    assert_obj(r)

    # ... assert http response code
    assert_integer(r.status_code, 200)

    # ... assert http response encoding
    assert_string(r.encoding, HTTP_CONTENT_ENCODING_UTF8, ignore_case=True)

    def assert_json():
        # ... assert http header
        assert_string(r.headers.get('content-type'), HTTP_CONTENT_TYPE_JSON,
                      startswith=True, ignore_case=True)

        # ... assert status
        response_data = r.json()
        assert_integer(response_data['status'], expected_status)

        # ... assert data
        if expected_status == 0 and expected_data:
            del response_data['status']
            if response_data_parser:
                response_data_parser(response_data)
            assert response_data == expected_data

        return response_data


    def assert_csv():
        # ... assert http header
        assert_string(r.headers.get('content-type'), HTTP_CONTENT_TYPE_CSV,
                      startswith=True, ignore_case=True)

        return r.text


    # Assert json content.
    if expected_content_type == HTTP_CONTENT_TYPE_JSON:
        return assert_json()

    # Assert csv content.
    elif expected_content_type == HTTP_CONTENT_TYPE_CSV:
        # NOTE - errors are returned in json format
        if r.headers.get('content-type').lower() == HTTP_CONTENT_TYPE_JSON:
            return assert_json()
        else:
            return assert_csv()


def is_api_up():
    """Returns flag indicating whether the API is currently up and running."""
    try:
        r = invoke_api(requests.get, _EP_OPS_HEARTBEAT)
    except requests.ConnectionError:
        return False
    else:
        return True