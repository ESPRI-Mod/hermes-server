# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.utils.config.py
   :copyright: Copyright "Feb 7, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Configuration utility functions.

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>


"""
# Module imports.
from os.path import dirname, abspath, join, exists

from . import runtime as rt
from . convert import (
	json_file_to_dict, 
	json_file_to_namedtuple
	)



# Module exports.
__all__ = [
	'api',
	'core',
	'data',
	'db',
	'mq',
	'set'
]



# Configuration data.
data = None

# API configuration data.
api = None

# Core configuration data.
core = None

# DB configuration data.
db = None

# MQ configuration data.
mq = None


def set(fp=None):
	"""Sets the configuration data wrapper.

	:param fp: Path to configuration file.
	:type fp: str

	"""
	if not exists(fp):
		raise RuntimeError("Configuration file does not exist :: {0}".format(fp))

	global api
	global core
	global data
	global db
	global mq

	# Cache pointers to config sections of interest.
	data = json_file_to_namedtuple(fp)
	api = data.api
	core = data.core
	db = data.db
	mq = data.mq


def get_mq_config(q):
	"""Returns mesage queue configuration.

	:param q: Name of message queue.
	:type q: str

	:returns: Message queue configuration.
	:rtype: namedtuple

	"""
	for cfg in mq.queues:
		if cfg.name == q:
			return cfg


def get_mq_config1(msg_type):
	"""Returns mesage queue configuration.

	:param str msg_type: Type of message.

	:returns: Message queue configuration.
	:rtype: namedtuple

	"""
	for cfg in mq.types:
		if cfg.code == msg_type:
			return cfg