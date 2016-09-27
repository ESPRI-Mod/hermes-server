# -*- coding: utf-8 -*-

"""
.. module:: hermes.utils.config.py
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
	# ... general variables
	"HERMES_HOME": "/opt/hermes",
	"HERMES_DEPLOYMENT_MODE": "test",
	"HERMES_CLIENT_WEB_URL": "https://hermes.upmc.ipsl.fr",
	"HERMES_ALERT_EMAIL_ADDRESS_FROM": None,
	"HERMES_ALERT_EMAIL_ADDRESS_TO": None,

	# ... db server variables
	"HERMES_DB_MONGO_HOST": "localhost:27017",
	"HERMES_DB_MONGO_USER_PASSWORD": None,
	"HERMES_DB_PGRES_HOST": "localhost:5432",
	"HERMES_DB_PGRES_NAME": "hermes",
	"HERMES_DB_PGRES_USER": "hermes_db_user",
	"HERMES_DB_PGRES_USER_PASSWORD": None,

	# ... mq server variables
	"HERMES_MQ_RABBIT_HOST": "localhost:5672",
	"HERMES_MQ_RABBIT_PROTOCOL": "ampq",
	"HERMES_MQ_RABBIT_USER_PASSWORD": None,
	"HERMES_MQ_RABBIT_SSL_CLIENT_CERT": None,
	"HERMES_MQ_RABBIT_SSL_CLIENT_KEY": None,
	"HERMES_MQ_IMAP_MAILBOX": "AMPQ-TEST",
	"HERMES_MQ_IMAP_MAILBOX_PROCESSED": "AMPQ-TEST-PROCESSED",
	"HERMES_MQ_IMAP_MAILBOX_REJECTED": "AMPQ-TEST-REJECTED",
	"HERMES_MQ_IMAP_PASSWORD": None,
	"HERMES_MQ_SMTP_PASSWORD": None,

	# ... web server variables
	"HERMES_WEB_COOKIE_SECRET": None,
	"HERMES_WEB_PORT": "8888",
	"HERMES_WEB_URL": "https://hermes.upmc.ipsl.fr"
}

# Set of environment variables to be url encoded.
_ENV_VARS_URL_ENCODE = {
	"HERMES_DB_MONGO_USER_PASSWORD",
	"HERMES_DB_PGRES_USER_PASSWORD"
}


def _get_env_var_value(var_name, var_default):
	"""Returns the formatted value of an environment variable.

	"""
	value = os.getenv(var_name, var_default)
	if value is None:
		return unicode()
	elif var_name in _ENV_VARS_URL_ENCODE:
		return urllib.quote_plus(value)
	return value


def _set_mq_ssl_options():
	"""Sets MQ server connection ssl options.

	"""
	# Initialise.
	_ENV_VARS['HERMES_MQ_RABBIT_SSL_OPTIONS'] = unicode()

	# Exit if cert/key undefined.
	if not _ENV_VARS['HERMES_MQ_RABBIT_SSL_CLIENT_CERT'] or \
	   not _ENV_VARS['HERMES_MQ_RABBIT_SSL_CLIENT_KEY']:
	   return

	# Set options.
	_ENV_VARS['HERMES_MQ_RABBIT_SSL_OPTIONS'] = urllib.urlencode({
		'ssl_options': {
			'certfile': _ENV_VARS['HERMES_MQ_RABBIT_SSL_CLIENT_CERT'],
			'keyfile': _ENV_VARS['HERMES_MQ_RABBIT_SSL_CLIENT_KEY']
			}
		})


def _init_env_vars():
	"""Initialises set of environment variables.

	"""
	# Set generic values.
	for var_name, var_default in _ENV_VARS.items():
		_ENV_VARS[var_name] = _get_env_var_value(var_name, var_default)

	# Set mq connection ssl options.
	_set_mq_ssl_options()


def _get_config_file_content():
	"""Load configuration file content from file system.

	"""
	path = _ENV_VARS["HERMES_HOME"]
	path = os.path.join(path, "ops")
	path = os.path.join(path, "config")
	path = os.path.join(path, "hermes.json")
	if not os.path.exists(path):
	    msg = "HERMES configuration file does not exist :: {}"
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

