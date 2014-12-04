# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.utils.config.py
   :copyright: Copyright "Feb 7, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Configuration utility functions.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import os

from .convert import json_file_to_namedtuple


# Default prodiguer stack home directory.
_DEFAULT_HOME = '/opt/prodiguer'

# Set home directory either from environment variable or from default.
try:
	_HOME = os.environ['PRODIGUER_HOME']
except KeyError:
	_HOME = _DEFAULT_HOME

# Configuration file name.
_CONFIG_FNAME = "prodiguer.json"

# Configuration file path.
_CONFIG_FPATH = "{0}/ops/config/{1}".format(_HOME, _CONFIG_FNAME)

# Exception if not found.
if not os.path.exists(_CONFIG_FPATH):
	msg = "PRODIGUER configuration file does not exist :: {0}"
	raise RuntimeError(msg.format(_CONFIG_FPATH))

# Config data wrapper.
data = json_file_to_namedtuple(_CONFIG_FPATH)
