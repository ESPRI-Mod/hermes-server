# -*- coding: utf-8 -*-

"""
.. module:: test_utils_convert.py

   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL / CeCILL
   :platform: Unix
   :synopsis: Encapsulates conversion utils tests.

.. moduleauthor:: IPSL (ES-DOC) <dev@esdocumentation.org>

"""
from os.path import abspath
from os.path import dirname
from os.path import exists
from os.path import join

from . import utils as tu
from prodiguer.utils import convert



# Config filename.
_CONFIG_FILENAME = 'config.json'


def _get_config_filepath():
	"""Returns the path to the HERMES configuration file.

	"""
	fp = abspath(__file__)
	for i in range(4):
		fp = dirname(fp)

	return join(fp, _CONFIG_FILENAME)


# Path to config file.
_CONFIG_FILEPATH = _get_config_filepath()


def test_config_file_path():
	assert _CONFIG_FILEPATH is not None
	assert exists(_CONFIG_FILEPATH)


def test_config_file_to_namedtuple():
	cfg = convert.json_file_to_namedtuple(_CONFIG_FILEPATH)
	tu.assert_namedtuple(cfg)
	tu.assert_namedtuple(cfg.web)
	tu.assert_obj(cfg.web.host, unicode)
	tu.assert_obj(cfg.web.port, int)
	tu.assert_obj(cfg.web.cookie_secret, unicode)	
	tu.assert_namedtuple(cfg.db)
	tu.assert_obj(cfg.db.connection, unicode)	
	tu.assert_obj(cfg.db.auto_initialize, bool)	
	tu.assert_namedtuple(cfg.mq)
	tu.assert_obj(cfg.mq.host, unicode)	
	tu.assert_obj(cfg.mq.auto_initialize, bool)	
