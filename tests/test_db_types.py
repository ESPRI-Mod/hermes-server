# -*- coding: utf-8 -*-

"""
.. module:: test_db_types.py

   :copyright: @2013 Institute Pierre Simon Laplace (http://esdocumentation.org)
   :license: GPL / CeCILL
   :platform: Unix
   :synopsis: Encapsulates db type tests.

.. moduleauthor:: Institute Pierre Simon Laplace (ES-DOC) <dev@esdocumentation.org>

"""
import inspect

from . import utils as tu


def test_db_types_import_package_01():
	from prodiguer import db
	assert inspect.ismodule(db.types)


def test_db_types_import_package_02():
	old = len(locals())
	from prodiguer.db.types import *
	new = len(locals())

	tu.assert_integer(new - old, 37)


def test_db_types_import_model_types():
	from prodiguer import db

	assert len(db.types.SCHEMAS) == 4
	assert len(db.types.TYPES) == 27
	assert len(db.types.CV) == 22
	assert len(db.types.CACHEABLE) == 13


def test_db_types_creation():
	from prodiguer.db.types import TYPES

	for type in TYPES:
		tu.assert_db_type_creation.description = "tests.test_db_types.test_creation :: {0}".format(type.__name__)
		yield tu.assert_db_type_creation, type


def test_db_types_conversion():
	from prodiguer.db.types import TYPES

	for type in TYPES:
		tu.assert_db_type_conversion.description = "tests.test_db_types.test_conversion :: {0}".format(type.__name__)
		yield tu.assert_db_type_conversion, type


def test_db_types_persistence():
	from prodiguer.db.types import TYPES

	for type in TYPES:
		tu.assert_db_type_persistence.description = "tests.test_db_types.test_persistence :: {0}".format(type.__name__)
		yield tu.assert_db_type_persistence, type
