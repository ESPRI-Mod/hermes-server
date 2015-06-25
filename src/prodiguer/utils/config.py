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
import urllib

from prodiguer.utils.convert import json_to_namedtuple


# Environment variables to be injected into config.
_ENV_VARS = {
	"PRODIGUER_DB_MONGO_HOST": "localhost:27017",
	"PRODIGUER_DB_MONGO_USER_PASSWORD": None,
	"PRODIGUER_DB_PGRES_HOST": "localhost:5432",
	"PRODIGUER_DB_PGRES_USER_PASSWORD": None,
	"PRODIGUER_HOME": "/opt/prodiguer",
	"PRODIGUER_MQ_IMAP_PASSWORD": None,
	"PRODIGUER_MQ_RABBIT_HOST": "localhost:5671",
	"PRODIGUER_MQ_RABBIT_LIBIGCM_USER_PASSWORD": None,
	"PRODIGUER_MQ_RABBIT_USER_PASSWORD": None,
	"PRODIGUER_MQ_SMTP_PASSWORD": None,
	"PRODIGUER_WEB_API_COOKIE_SECRET": None,
	"PRODIGUER_WEB_HOST": "localhost:8888",
	"PRODIGUER_WEB_URL": "https://prodiguer-test-web.ipsl.fr"
}

# Set of environment variables to be url encoded.
_ENV_VARS_URL_ENCODE = {
	"PRODIGUER_DB_MONGO_USER_PASSWORD",
	"PRODIGUER_DB_PGRES_USER_PASSWORD"
}


def _get_env_var_value(var_name, var_default):
	"""Returns the formatted value of an environment variable.

	"""
	value = os.getenv(var_name, var_default)
	if value is None:
		return ""
	elif var_name in _ENV_VARS_URL_ENCODE:
		return urllib.quote_plus(value)
	return value


def _init_env_vars():
	"""Initialises set of environment variables.

	"""
	for var_name, var_default in _ENV_VARS.items():
		_ENV_VARS[var_name] = _get_env_var_value(var_name, var_default)


def _get_config_file_content():
	"""Load configuration file content from file system.

	"""
	path = _ENV_VARS["PRODIGUER_HOME"]
	path = os.path.join(path, "ops")
	path = os.path.join(path, "config")
	path = os.path.join(path, "prodiguer.json")
	if not os.path.exists(path):
	    msg = "PRODIGUER configuration file does not exist :: {}"
	    raise RuntimeError(msg.format(path))

	with open(path, 'r') as data:
		return data.read()


def _get_config_file_data():
	"""Initialises configuration data.

	"""
	_init_env_vars()
	content = _get_config_file_content()
	for name, value in _ENV_VARS.items():
		content = content.replace("{{{}}}".format(name), value or '')

	return json_to_namedtuple(content)


# Config data wrapper.
data = _get_config_file_data()
