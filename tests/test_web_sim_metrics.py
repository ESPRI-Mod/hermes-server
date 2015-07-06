# -*- coding: utf-8 -*-

"""
.. module:: test_web_sim_metrics.py

   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL / CeCILL
   :platform: Unix
   :synopsis: Simulation metrics web service tests.

.. moduleauthor:: IPSL (ES-DOC) <dev@esdocumentation.org>

"""
import uuid

import nose
import requests

import prodiguer
from . import _utils as tu



# Web service endpoints.
_EP_BASE = r"http://localhost:8888/api/1/simulation/metrics/{}"
_EP_ADD = _EP_BASE.format("add")
_EP_DELETE = _EP_BASE.format("delete?group={0}")
_EP_FETCH = _EP_BASE.format("fetch?group={0}")
_EP_FETCH_COLUMNS = _EP_BASE.format("fetch_columns?group={0}")
_EP_FETCH_COUNT = _EP_BASE.format("fetch_count?group={0}")
_EP_FETCH_LIST = _EP_BASE.format("fetch_list")
_EP_FETCH_SETUP = _EP_BASE.format("fetch_setup?group={0}")
_EP_RENAME = _EP_BASE.format("rename?group={0}&new_name={1}")

# Existing metric groups.
_GROUPS = set()

# Group name length constraints.
_GROUP_NAME_MIN_LENGTH = 4
_GROUP_NAME_MAX_LENGTH = 256



def _on_test_setup():
	"""Test setup.

	"""
	r = tu.invoke_api(requests.get, _EP_FETCH_LIST)
	r = r.json()
	_GROUPS.update(r['groups'])


def _on_test_teardown():
	"""Test teardown.

	"""
	r = tu.invoke_api(requests.get, _EP_FETCH_LIST)
	r = r.json()
	for group in [g for g in r['groups'] if g not in _GROUPS]:
		tu.invoke_api(requests.post, _EP_DELETE.format(group))


def _get_metrics():
	"""Returns a set of valid metrics.

	"""
	return {
		u'columns': [
			u'a', u'b', u'c', u'd', u'e', u'f', '_id'
		],
		u'group': tu.get_string(12),
		u'metrics': [
			[1, 2, 3, 4, 5, 6, unicode(uuid.uuid4())],
			[1, 2, 3, 4, 5, 6, unicode(uuid.uuid4())],
			[1, 2, 3, 4, 5, 6, unicode(uuid.uuid4())],
			[1, 2, 3, 4, 5, 6, unicode(uuid.uuid4())],
			[1, 2, 3, 4, 5, 6, unicode(uuid.uuid4())],
			[1, 2, 3, 4, 5, 6, unicode(uuid.uuid4())],
		]
	}


def _yield_invalid_metrics():
	"""Yield invalid metrics for testing purposes.

	"""
	def _get_invalid_metrics_01():
		"""Returns a set of metrics with an invalid group name."""
		data = _get_metrics()
		data['group'] = u'test-#$%'

		return data


	def _get_invalid_metrics_02():
		"""Returns a set of metrics with an invalid group name."""
		data = _get_metrics()
		data['group'] = u""
		for i in range(_GROUP_NAME_MIN_LENGTH - 1):
			data['group'] += u"A"

		return data


	def _get_invalid_metrics_03():
		"""Returns a set of metrics with an invalid group name."""
		data = _get_metrics()
		data['group'] = u""
		for i in range(_GROUP_NAME_MAX_LENGTH + 1):
			data['group'] += u"A"

		return data


	def _get_invalid_metrics_04():
		"""Returns a set of metrics with a column mismatch."""
		data = _get_metrics()
		data['metrics'] = [
			[1, 2, 3, 4, 5],
			[1, 2, 3, 4, 5, 6],
		]

		return data


	def _get_invalid_metrics_05():
		"""Returns a set of metrics with an invalid column name."""
		data = _get_metrics()
		data['columns'] = data['columns'][:5] + ["metric_id"]

		return data

	# Yield factory functions.
	yield _get_invalid_metrics_01
	yield _get_invalid_metrics_02
	yield _get_invalid_metrics_03
	yield _get_invalid_metrics_04
	yield _get_invalid_metrics_05


def _assert_fetch_count(r, m):
	"""Asserts web service endpoint response: fetch-count.

	"""
	response = tu.assert_api_response(r)
	tu.assert_string(m['group'], response['group'])
	tu.assert_integer(len(m['metrics']), int(response['count']))


def _assert_fetch_columns(r, m):
	"""Asserts web service endpoint response: fetch-columns.

	"""
	expected = m.copy()
	del expected['metrics']

	tu.assert_api_response(r, expected_data=expected)


def _assert_fetch_list(r, old_list=None, diff=1):
	"""Asserts web service endpoint response: fetch-list.

	"""
	response = tu.assert_api_response(r)
	if old_list:
		new_list = response['groups']
		for item in old_list:
			assert item in new_list
		if diff:
			tu.assert_integer(len(new_list), len(old_list) + diff)


def _assert_fetch_setup(r, m):
	"""Asserts web service endpoint response: fetch-setup.

	"""
	expected = m.copy()
	del expected['metrics']
	expected['data'] = [set() for _ in m['columns']]
	for row in m['metrics']:
		for i in range(0, len(m['columns']) - 1):
			expected['data'][i].add(row[i])
	expected['data'] = [list(i) for i in expected['data']]
	response = tu.assert_api_response(r)


def _test_positive():
	"""testing sim-metrics web service: postive

	"""
	# Fetch list.
	r = tu.invoke_api(requests.get, _EP_FETCH_LIST)
	_assert_fetch_list(r)
	m_list = r.json()['groups']

	# Add.
	m = _get_metrics()
	r = tu.invoke_api(requests.post, _EP_ADD, m)
	tu.assert_api_response(r)

	# Fetch.
	r = tu.invoke_api(requests.get, _EP_FETCH.format(m['group']))
	tu.assert_api_response(r, expected_data=m)
	m_fetched = r.json()

	# Fetch columns.
	r = tu.invoke_api(requests.get, _EP_FETCH_COLUMNS.format(m['group']))
	_assert_fetch_columns(r, m)

	# Fetch count.
	r = tu.invoke_api(requests.get, _EP_FETCH_COUNT.format(m['group']))
	_assert_fetch_count(r, m)

	# # Fetch list.
	r = tu.invoke_api(requests.get, _EP_FETCH_LIST)
	_assert_fetch_list(r, old_list=m_list, diff=1)

	# Fetch setup.
	r = tu.invoke_api(requests.get, _EP_FETCH_SETUP.format(m['group']))
	_assert_fetch_setup(r, m)

	# Rename.
	new_name = tu.get_string(12)
	r = tu.invoke_api(requests.post, _EP_RENAME.format(m['group'], new_name))
	tu.assert_api_response(r)

	# Delete.
	r = tu.invoke_api(requests.post, _EP_DELETE.format(new_name))
	tu.assert_api_response(r)

	# Fetch list.
	r = tu.invoke_api(requests.get, _EP_FETCH_LIST)
	_assert_fetch_list(r, old_list=m_list, diff=0)

	# Fetch.
	r = tu.invoke_api(requests.get, _EP_FETCH.format(new_name))
	tu.assert_integer(r.status_code, 400)


def _test_negative():
	"""testing sim-metrics web service: negative

	"""
	for m in (f() for f in _yield_invalid_metrics()):
		r = tu.invoke_api(requests.post, _EP_ADD, m)
		tu.assert_integer(r.status_code, 400)


@nose.tools.with_setup(setup=_on_test_setup, teardown=_on_test_teardown)
def test():
	"""Tests sim-metrics web service.

	"""
	for test in (
		_test_positive,
		_test_negative
		):
		test.description = test.__doc__.strip()
		yield test
