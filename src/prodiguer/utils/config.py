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
import os

from . import convert



# Module exports.
__all__ = [
	'api',
	'core',
	'data',
	'db',
	'mq',
	'set'
]



# Standard configuration file path.
_CONFIG="{0}/.prodiguer".format(os.environ['HOME'])

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


def init(fp=_CONFIG):
	"""Initializes configuration.

	:param str fp: Path to configuration file.

	"""
	if not os.path.exists(fp):
		raise RuntimeError("Configuration file does not exist :: {0}".format(fp))

	global api
	global core
	global data
	global db
	global mq

	# Cache pointers to config sections of interest.
	data = convert.json_file_to_namedtuple(fp)
	api = data.api
	core = data.core
	db = data.db
	mq = data.mq
