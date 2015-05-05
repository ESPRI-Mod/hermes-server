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
from os.path import dirname, join, abspath, exists

from . import utils as tu



# Config filename.
_CONFIG_FILENAME = 'test_files/config.json'

# Config filepath.
_CONFIG_FILEPATH = join(dirname(abspath(__file__)), _CONFIG_FILENAME)


def _assert_configuration(data):
	tu.assert_namedtuple(data)
	tu.assert_namedtuple(data.api)
	tu.assert_namedtuple(data.db)
	tu.assert_namedtuple(data.mq)


def test_main_import():
	"""Tests top-level package import."""
	import prodiguer

	modules = [
		prodiguer,
		prodiguer.web,
		prodiguer.config,
		prodiguer.cv,
		prodiguer.db,
		# prodiguer.mq,
		prodiguer.utils
	]

	for module in modules:
		tu.assert_bool(inspect.ismodule(module))
	tu.assert_bool(inspect.isfunction(prodiguer.configure))


def test_main_configure():
	"""Tests custom library configuration."""
	import prodiguer

	prodiguer.configure(_CONFIG_FILEPATH)

	_assert_configuration(prodiguer.config.data)


def test_main_configuration_default_data():
	"""Tests default library configuration."""
	import prodiguer

	_assert_configuration(prodiguer.config.data)
