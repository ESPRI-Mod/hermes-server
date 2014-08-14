# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.mq.consumers.api_sim_mon.py
   :copyright: Copyright "Apr 26, 2013", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Consumes messages targeting simulation monitoring api.

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>


"""
# Module imports.
import requests

from ... api.utils import ep
from ... utils import runtime as rt



# Message queue to which I am bound.
MQ = "api"

# API endpoint to post event data to.
_API_EP = '/monitoring/event'

# API not running error message.
_ERR_API_NOT_RUNNING = "API service needs to be started."


def _get_api_ep():
	"""Returns target api endpoint."""
	return ep.get_endpoint(_API_EP)


def _post_to_web_app(event_info):
	"""Posts event information to web app."""
	try:
		ep = _get_api_ep()
		requests.get(ep, params=event_info)
	except requests.exceptions.ConnectionError:
		rt.log_api(_ERR_API_NOT_RUNNING, level=rt.LOG_LEVEL_WARNING)
	except Exception as e:
		rt.log_error(e)


def consume(msg):
	"""Message consumption entry point."""
	_post_to_web_app(msg)

