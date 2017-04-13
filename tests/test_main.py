# -*- coding: utf-8 -*-

"""
.. module:: test_cv.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Encapsulates cv tests.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import inspect
from os.path import abspath
from os.path import dirname
from os.path import join

from . import utils as tu



# Config filename.
_CONFIG_FILENAME = 'testConfig.json'

# Config filepath.
_CONFIG_FILEPATH = join(dirname(abspath(__file__)), _CONFIG_FILENAME)


def _assert_configuration(data):
	tu.assert_namedtuple(data)
	tu.assert_namedtuple(data.api)
	tu.assert_namedtuple(data.db)
	tu.assert_namedtuple(data.mq)


def test_main_import():
	"""Tests top-level package import."""
	import hermes

	modules = [
		hermes,
		hermes.web,
		hermes.config,
		hermes.cv,
		hermes.db,
		# hermes.mq,
		hermes.utils
	]

	for module in modules:
		tu.assert_bool(inspect.ismodule(module))
	tu.assert_bool(inspect.isfunction(hermes.configure))


def test_main_configure():
	"""Tests custom library configuration."""
	import hermes

	hermes.configure(_CONFIG_FILEPATH)

	_assert_configuration(hermes.config.data)


def test_main_configuration_default_data():
	"""Tests default library configuration."""
	import hermes

	_assert_configuration(hermes.config.data)
