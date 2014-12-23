# -*- coding: utf-8 -*-

"""
.. module:: test_api_ws_utils_ws.py

   :copyright: @2013 IPSL (http://esdocumentation.org)
   :license: GPL / CeCILL
   :platform: Unix
   :synopsis: Encapsulates api web socket ws_utils tests.

.. moduleauthor:: IPSL (ES-DOC) <dev@esdocumentation.org>

"""
import nose 

from . import utils as tu
from prodiguer.api.utils import ws as ws_utils



_TEST_WS_KEY = "test"
_TEST_WS_MSG = "{ 'x': 123 }"


class _MockWebSocket(object):
	def __init__(self):
		self.messages = []
		self.last_message = None

	def write_message(self, msg):
		self.last_message = msg
		self.messages.append(msg)


class _MockWebSocketWithFault(object):
	def write_message(self, msg):
		raise ValueError(msg)


def _ws_test_teardown():
	ws_utils.clear_cache()


@nose.with_setup(None, _ws_test_teardown)
def test_api_ws_on_connect():
	key, ws = _TEST_WS_KEY, _MockWebSocket()

	ws_utils.on_connect(key, ws)
	tu.assert_integer(ws_utils.get_client_count(key), 1)


@nose.with_setup(None, _ws_test_teardown)
def test_api_ws_on_disconnect():
	key, ws = _TEST_WS_KEY, _MockWebSocket()
	ws_utils.on_connect(key, ws)

	ws_utils.on_disconnect(key, ws)
	tu.assert_integer(ws_utils.get_client_count(key), 0)


def test_api_ws_clear_cache_01():
	keys = "ABC"
	for key in keys:
		ws_utils.on_connect(key, _MockWebSocket())	
	tu.assert_integer(ws_utils.get_client_count(), 3)

	for key in keys:
		tu.assert_integer(ws_utils.get_client_count(key), 1)
	
	for key in keys:
		ws_utils.clear_cache(key)
		tu.assert_integer(ws_utils.get_client_count(key), 0)


def test_api_ws_clear_cache_02():
	keys = "ABC"
	for key in keys:
		ws_utils.on_connect(key, _MockWebSocket())	
	tu.assert_integer(ws_utils.get_client_count(), 3)

	ws_utils.clear_cache()
	tu.assert_integer(ws_utils.get_client_count(), 0)


@nose.with_setup(None, _ws_test_teardown)
def test_api_ws_on_write():
	msg_list = "ABC"
	key, ws = _TEST_WS_KEY, _MockWebSocket()
	ws_utils.on_connect(key, ws)

	for msg in msg_list:
		ws_utils.on_write(_TEST_WS_KEY, msg)
		tu.assert_string(ws.last_message, msg)
	tu.assert_collection(ws.messages, 3)


@nose.with_setup(None, _ws_test_teardown)
def test_api_ws_ws_write_fault():
	msg_list = "ABC"
	key, ws = _TEST_WS_KEY, _MockWebSocketWithFault()
	ws_utils.on_connect(key, ws)

	for msg in msg_list:
		try:
			ws_utils.on_write(_TEST_WS_KEY, msg)
		except ValueError as e:
			tu.assert_string(msg, e.message)

