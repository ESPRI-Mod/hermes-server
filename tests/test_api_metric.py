# -*- coding: utf-8 -*-

"""
.. module:: test_api_simulation_metrics.py

   :copyright: @2013 Institute Pierre Simon Laplace (http://esdocumentation.org)
   :license: GPL / CeCILL
   :platform: Unix
   :synopsis: Encapsulates simulation metrics api tests.

.. moduleauthor:: Institute Pierre Simon Laplace (ES-DOC) <dev@esdocumentation.org>

"""
from multiprocessing import Process

import nose
import requests

from prodiguer import api
from . import (
	utils as tu,
	utils_api_metric as tu_api
	)



# Module state wrapper.
class _State():
	api = None
	api_is_up = tu.is_api_up()


def _on_test_setup():
	"""Test setup."""
	if not _State.api_is_up:
		_State.api = Process(target=api.run)
		_State.api.start()
		_State.api_is_up = True


def _on_test_teardown():
	"""Test teardown."""
	if _State.api:
		_State.api.terminate()				
		_State.api = None


@nose.tools.with_setup(setup=_on_test_setup, teardown=_on_test_teardown)
def test():
	for test in (
		_test_positive, 
		_test_negative, 
		):
		test.description = test.__doc__.lower()
		yield test


def _test_positive():
	"""testing api: metric - positive"""
	# Instantiate a test metric.
	m = tu_api.get_valid_metrics()

	# Get current list.
	r = tu.invoke_api(requests.get, tu_api.EP_LIST_GROUP)
	m_list = r.json()['groups']

	# Test metric API add method.
	r = tu.invoke_api(requests.post, tu_api.EP_ADD, m)
	tu_api.assert_response_add(r)

	# Test metric API fetch method (format = json).
	r = tu.invoke_api(requests.get, tu_api.EP_FETCH.format(m['group']))
	tu_api.assert_response_fetch(r, m)
	m_fetched = r.json()

	# Test metric API fetch method (format = csv).
	r = tu.invoke_api(requests.get, tu_api.EP_FETCH_CSV.format(m['group']))
	tu_api.assert_response_fetch(r, m, format=tu.ENCODING_CSV)

	# Test metric API fetch method (headersonly).
	r = tu.invoke_api(requests.get, tu_api.EP_FETCH_HEADERS.format(m['group']))
	tu_api.assert_response_fetch_headers(r, m)

	# Test metric API list method.
	r = tu.invoke_api(requests.get, tu_api.EP_LIST_GROUP)
	tu_api.assert_response_list(r, old_list=m_list)

	# Test metric API delete group method.
	r = tu.invoke_api(requests.post, tu_api.EP_DELETE_GROUP.format(m['group']))
	tu_api.assert_response_delete_group(r)

	# Confirm deletion by checking list.
	r = tu.invoke_api(requests.get, tu_api.EP_LIST_GROUP)
	tu_api.assert_response_list(r, old_list=m_list, diff=0)

	# Confirm deletion by checking fetch.
	r = tu.invoke_api(requests.get, tu_api.EP_FETCH.format(m['group']))
	tu_api.assert_response_fetch(r, expected_status=1)


def _test_negative():
	"""testing api: metric - negative"""
	for metric in tu_api.get_invalid_metrics():
		r = tu.invoke_api(requests.post, tu_api.EP_ADD, metric)
		tu_api.assert_response_add(r, 1)
