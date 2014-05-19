# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.mq.controller.py
   :copyright: Copyright "Apr 26, 2013", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Message queue system entry point.

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>


"""
# Module imports.
from . import constants, controller, utils
from .consumers import (
	api_consumer, 
	main_consumer
	)
from .producers import (
	api_producer, 
	asynch_producer,
	main_producer_mock
	)


# Initialization.
def initialize():
	"""Initialises sub-package.

	"""
	controller.initialise({
		'api' : (api_producer, api_consumer),
		'asynch': (asynch_producer, None),
		'main' : (main_producer_mock, main_consumer)
		})

