# -*- coding: utf-8 -*-

"""
.. module:: test_utils_convert.py

   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL / CeCILL
   :platform: Unix
   :synopsis: Encapsulates conversion utils tests.

.. moduleauthor:: IPSL (ES-DOC) <dev@esdocumentation.org>

"""
import datetime
import uuid
from os.path import abspath
from os.path import dirname
from os.path import exists

from . import utils as tu
from prodiguer.utils import convert


# Path to test json file.
_JSON_FILEPATH = "{0}/{1}".format(dirname(abspath(__file__)), "/testObject.json")



def _test_str(f, values):
	for i, o in values:
		tu.assert_string(f(i), o)


def test_convert_json_file_path():
	assert _JSON_FILEPATH is not None
	assert exists(_JSON_FILEPATH)


def test_convert_json_file_to_dict_01():
	d = convert.json_file_to_dict(_JSON_FILEPATH)

	tu.assert_obj(d, dict)
	tu.assert_obj(d['test_bool'], bool)	
	tu.assert_obj(d['test_datetime'], datetime.datetime)	
	tu.assert_obj(d['test_float'], float)	
	tu.assert_obj(d['test_id'], int)	
	tu.assert_obj(d['test_int'], int)	
	tu.assert_obj(d['test_list'], list)	
	tu.assert_obj(d['test_list'][0], int)	
	tu.assert_obj(d['test_list_dict'], list)	
	tu.assert_obj(d['test_list_dict'][0], dict)	
	tu.assert_obj(d['test_list_dict'][0]['s'], unicode)	
	tu.assert_obj(d['test_dict'], dict)	
	tu.assert_obj(d['test_dict']['a'], unicode)	
	tu.assert_obj(d['test_dict']['b'], list)	
	tu.assert_obj(d['test_dict']['c'], dict)	
	tu.assert_obj(d['test_dict']['c']['x'], bool)	
	tu.assert_none(d['test_none'])	
	tu.assert_obj(d['test_uid'], uuid.UUID)	
	tu.assert_obj(d['test_uuid'], uuid.UUID)	
	tu.assert_obj(d['test_unicode'], unicode)	

	assert d['test_bool'] == True
	assert d['test_datetime']
	assert d['test_float'] == 1.23
	assert d['test_id'] == 123
	assert d['test_int'] == 123
	assert d['test_list'] == [1, 2, 3]
	for x in d['test_list_dict']:
		assert x == {"s": "ABC"}
	assert d['test_none'] == None
	assert d['test_dict']['a'] == "test"
	assert len(d['test_dict']['b']) == 2
	assert d['test_dict']['b'][0]['x'] == True
	assert d['test_dict']['c']['x'] == True
	assert d['test_uid'] == uuid.UUID("29d79d5e-5765-4d63-8059-0ad1b92628cc")
	assert d['test_uuid'] == uuid.UUID("29d79d5e-5765-4d63-8059-0ad1b92628cc")
	assert d['test_unicode'] == unicode("abc")


def test_convert_json_file_to_namedtuple():
	t = convert.json_file_to_namedtuple(_JSON_FILEPATH)
	tu.assert_namedtuple(t)

	tu.assert_obj(t.test_bool, bool)	
	tu.assert_obj(t.test_datetime, datetime.datetime)	
	tu.assert_obj(t.test_float, float)	
	tu.assert_obj(t.test_id, int)	
	tu.assert_obj(t.test_int, int)	
	tu.assert_obj(t.test_list, list)	
	tu.assert_obj(t.test_list[0], int)	
	tu.assert_obj(t.test_list_dict, list)	
	tu.assert_namedtuple(t.test_list_dict[0])
	tu.assert_obj(t.test_list_dict[0].s, unicode)	
	tu.assert_namedtuple(t.test_dict)
	tu.assert_obj(t.test_dict.a, unicode)	
	tu.assert_obj(t.test_dict.b, list)	
	tu.assert_namedtuple(t.test_dict.c)
	tu.assert_obj(t.test_dict.c.x, bool)	
	tu.assert_none(t.test_none)	
	tu.assert_obj(t.test_uid, uuid.UUID)	
	tu.assert_obj(t.test_uuid, uuid.UUID)	
	tu.assert_obj(t.test_unicode, unicode)	

	assert t.test_bool == True
	assert t.test_datetime
	assert t.test_float == 1.23
	assert t.test_id == 123
	assert t.test_int == 123
	assert t.test_list == [1, 2, 3]
	for x in t.test_list_dict:
		assert x.s == "ABC"
	assert t.test_none == None
	assert t.test_dict.a == "test"
	assert len(t.test_dict.b) == 2
	assert t.test_dict.b[0].x == True
	assert t.test_dict.c.x == True
	assert t.test_uid == uuid.UUID("29d79d5e-5765-4d63-8059-0ad1b92628cc")
	assert t.test_uuid == uuid.UUID("29d79d5e-5765-4d63-8059-0ad1b92628cc")
	assert t.test_unicode == unicode("abc")


def test_convert_json_file_to_namedtuple_with_keys_formatted_as_camel_case():
	t = convert.json_file_to_namedtuple(_JSON_FILEPATH, convert.str_to_camel_case)
	tu.assert_namedtuple(t)

	tu.assert_obj(t.testBool, bool)	
	tu.assert_obj(t.testDatetime, datetime.datetime)	
	tu.assert_obj(t.testFloat, float)	
	tu.assert_obj(t.testID, int)	
	tu.assert_obj(t.testInt, int)	
	tu.assert_obj(t.testList, list)	
	tu.assert_obj(t.testList[0], int)	
	tu.assert_obj(t.testListDict, list)	
	tu.assert_namedtuple(t.testListDict[0])
	tu.assert_obj(t.testListDict[0].s, unicode)	
	tu.assert_namedtuple(t.testDict)
	tu.assert_obj(t.testDict.a, unicode)	
	tu.assert_obj(t.testDict.b, list)	
	tu.assert_namedtuple(t.testDict.c)
	tu.assert_obj(t.testDict.c.x, bool)	
	tu.assert_none(t.testNone)	
	tu.assert_obj(t.testUID, uuid.UUID)	
	tu.assert_obj(t.testUUID, uuid.UUID)	
	tu.assert_obj(t.testUnicode, unicode)	


def test_convert_json_file_to_namedtuple_with_keys_formatted_as_pascal_case():
	t = convert.json_file_to_namedtuple(_JSON_FILEPATH, convert.str_to_pascal_case)
	tu.assert_namedtuple(t)

	tu.assert_obj(t.TestBool, bool)	
	tu.assert_obj(t.TestDatetime, datetime.datetime)	
	tu.assert_obj(t.TestFloat, float)	
	tu.assert_obj(t.TestID, int)	
	tu.assert_obj(t.TestInt, int)	
	tu.assert_obj(t.TestList, list)	
	tu.assert_obj(t.TestList[0], int)	
	tu.assert_obj(t.TestListDict, list)	
	tu.assert_namedtuple(t.TestListDict[0])
	tu.assert_obj(t.TestListDict[0].S, unicode)	
	tu.assert_namedtuple(t.TestDict)
	tu.assert_obj(t.TestDict.A, unicode)	
	tu.assert_obj(t.TestDict.B, list)	
	tu.assert_namedtuple(t.TestDict.C)
	tu.assert_obj(t.TestDict.C.X, bool)	
	tu.assert_none(t.TestNone)	
	tu.assert_obj(t.TestUID, uuid.UUID)	
	tu.assert_obj(t.TestUUID, uuid.UUID)	
	tu.assert_obj(t.TestUnicode, unicode)	


def test_convert_str_to_pascal_case():
	_test_str(convert.str_to_pascal_case, (
		("test_name", "TestName"),
		("testName", "TestName"),
		("test_name_name", "TestNameName"),
		("TestNameName", "TestNameName"),
		("test_ID", "TestID"),
		("Test_ID", "TestID"),
		("testID", "TestID"),
		("test_UID", "TestUID"),
		("Test_UID", "TestUID"),
		("testUID", "TestUID"),
		("test_UUID", "TestUUID"),
		("Test_UUID", "TestUUID"),
		("testUUID", "TestUUID"),
		))


def test_convert_str_to_camel_case():
	_test_str(convert.str_to_camel_case, (
		("test_name", "testName"),
		("testName", "testName"),
		("test_name_name", "testNameName"),
		("TestNameName", "testNameName"),
		("test_ID", "testID"),
		("Test_ID", "testID"),
		("testID", "testID"),
		("test_UID", "testUID"),
		("Test_UID", "testUID"),
		("testUID", "testUID"),
		("test_UUID", "testUUID"),
		("Test_UUID", "testUUID"),
		("testUUID", "testUUID"),
		))


def test_convert_str_to_spaced_case():
	_test_str(convert.str_to_spaced_case, (
		("test_name", "test name"),
		("testName", "test Name"),
		("test_name_name", "test name name"),
		("TestNameName", "Test Name Name"),
		("test_ID", "test ID"),
		("Test_ID", "Test ID"),
		("testID", "test I D"),
		("test_UID", "test UID"),
		("Test_UID", "Test UID"),
		("testUID", "test U I D"),
		("test_UUID", "test UUID"),
		("Test_UUID", "Test UUID"),
		("testUUID", "test U U I D"),
		))


def test_convert_str_to_underscore_case():
	_test_str(convert.str_to_underscore_case, (
		("test_name", "test_name"),
		("testName", "test_name"),
		("test_name_name", "test_name_name"),
		("TestNameName", "test_name_name"),
		("test_ID", "test_id"),
		("Test_ID", "test_id"),
		("testID", "test_id"),
		("test_UID", "test_uid"),
		("Test_UID", "test_uid"),
		("testUID", "test_uid"),
		("test_UUID", "test_uuid"),
		("Test_UUID", "test_uuid"),
		("testUUID", "test_uuid"),
		))
