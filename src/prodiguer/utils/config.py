# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.utils.config.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Configuration utility functions.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import os

from prodiguer.utils.convert import json_file_to_namedtuple



# Set home directory either from environment variable or from default.
_HOME = os.getenv('PRODIGUER_HOME', '/opt/prodiguer')

# Configuration file path.
_CONFIG_FPATH = os.path.join(_HOME, "ops")
_CONFIG_FPATH = os.path.join(_CONFIG_FPATH, "config")
_CONFIG_FPATH = os.path.join(_CONFIG_FPATH, "prodiguer.json")

# Exception if not found.
if not os.path.exists(_CONFIG_FPATH):
    msg = "PRODIGUER configuration file does not exist :: {}"
    raise RuntimeError(msg.format(_CONFIG_FPATH))

# Config data wrapper.
data = json_file_to_namedtuple(_CONFIG_FPATH)

