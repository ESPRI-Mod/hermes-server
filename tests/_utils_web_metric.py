# -*- coding: utf-8 -*-

"""
.. module:: utils_api_metric.py

   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL / CeCILL
   :platform: Unix
   :synopsis: Simulation metrics api utilty test functions.

.. moduleauthor:: IPSL (ES-DOC) <dev@esdocumentation.org>

"""
import json
import random

from . import _utils as tu
from prodiguer.utils import convert



# Web service endpoints.
_EP_BASE = r"http://localhost:8888/api/1/simulation/metrics/{}"
EP_ADD = _EP_BASE.format("add")
EP_FETCH = _EP_BASE.format("fetch?group={0}")
EP_FETCH_COLUMNS = _EP_BASE.format("fetch_columns?group={0}")
EP_FETCH_LIST = _EP_BASE.format("fetch_list")


EP_DELETE_LINES = _EP_BASE + r"/delete"
EP_DELETE_GROUP = _EP_BASE + r"/delete_group?group={0}"



def get_valid_metrics():
	"""Returns a set of valid metrics.

	"""
	return {
		u'columns': [
			u'a', u'b', u'c', u'd', u'e', u'f'
		],
		u'group': tu.get_string(12),
		u'metrics': [
			[1, 2, 3, 4, 5, 6],
			[1, 2, 3, 4, 5, 6],
			[1, 2, 3, 4, 5, 6],
			[1, 2, 3, 4, 5, 6],
			[1, 2, 3, 4, 5, 6],
			[1, 2, 3, 4, 5, 6],
		]
	}


def get_metric_lines_for_deletion(m, line_count=3):
	return {
		'metric_id_list': random.sample(map(lambda line: line[-1], m['metrics']), line_count)
	}


def _get_invalid_metrics_01():
	"""Returns a set of metrics with an invalid group name."""	
	data = get_valid_metrics()
	data['group'] = u'test-#$%'

	return data


def _get_invalid_metrics_02():
	"""Returns a set of metrics with an invalid group name."""	
	data = get_valid_metrics()
	group = u""
	for i in range(3):
		group += u"A"
	data['group'] = group

	return data


def _get_invalid_metrics_03():
	"""Returns a set of metrics with an invalid group name."""	
	data = get_valid_metrics()
	group = u""
	for i in range(257):
		group += u"A"
	data['group'] = group

	return data


def _get_invalid_metrics_04():
	"""Returns a set of metrics with a column mismatch."""	
	data = get_valid_metrics()
	data['metrics'] = [
		[1, 2, 3, 4, 5],
		[1, 2, 3, 4, 5, 6],
	]

	return data


def _get_invalid_metrics_05():
	"""Returns a set of metrics with an invalid column name."""	
	data = get_valid_metrics()
	data['columns'] = data['columns'][:5] + ["metric_id"]

	return data


def get_invalid_metrics():
	"""Returns set of invalid metrics for testing purposes."""	
	return (
		_get_invalid_metrics_01(),
		_get_invalid_metrics_02(),
		_get_invalid_metrics_03(),
		_get_invalid_metrics_04(),
		_get_invalid_metrics_05(),
		)


def _fetch_response_data_parser(response_data):
	"""Parsers response data in readiness for assertions."""
	def shrink(a):
		return a[0:len(a) - 1]

	# Strip metric_id as these cannot be a-priori asserted.
	if 'metrics' in response_data:
		response_data['metrics'] = map(shrink, response_data['metrics'])
		response_data['columns'] = shrink(response_data['columns'])


def assert_response_add(r, expected_status=0):
	tu.assert_api_response(r, expected_status)


def assert_response_fetch(r, metric=None, expected_status=0, format='json'):
	if format == tu.ENCODING_JSON:
		tu.assert_api_response(r, 
							   expected_status, 
							   expected_data=metric, 
							   response_data_parser=_fetch_response_data_parser)
	
	elif format == tu.ENCODING_CSV:
		tu.assert_api_response(r, 
							   expected_status, 
							   expected_content_type=tu.HTTP_CONTENT_TYPE_CSV)


def assert_response_fetch_columns(r, m, expected_status=0):
	metric = m.copy()
	if expected_status == 0:
		del metric['metrics']
		metric['columns'].append('_id')
	tu.assert_api_response(r, expected_status, expected_data=metric)


def assert_response_delete_group(r, expected_status=0):
	tu.assert_api_response(r, expected_status)


def assert_response_list(r, expected_status=0, old_list=None, diff=1):
	new_list = tu.assert_api_response(r, expected_status)['groups']
	if old_list:
		for item in old_list:
			assert item in new_list 
		if diff:
			tu.assert_integer(len(new_list), len(old_list) + diff)
