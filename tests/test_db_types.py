# -*- coding: utf-8 -*-

"""
.. module:: test_db_types.py

   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL / CeCILL
   :platform: Unix
   :synopsis: Encapsulates db type tests.

.. moduleauthor:: IPSL (ES-DOC) <dev@esdocumentation.org>

"""
import inspect

from . import utils as tu


def test_db_types_import_package_01():
	from hermes import db
	assert inspect.ismodule(db.pgres.types)


def test_db_types_import_package_02():
	old = len(locals())
	from hermes.db.pgres.types import *
	new = len(locals())

	tu.assert_integer(new - old, 37)


def test_db_types_import_model_types():
	from hermes import db

	assert len(db.pgres.types.SCHEMAS) == 4
	assert len(db.pgres.types.SUPPORTED) == 27


def test_db_types_creation():
	from hermes.db.pgres.types import SUPPORTED

	for type in SUPPORTED:
		tu.assert_db_type_creation.description = "tests.test_db_types.test_creation :: {0}".format(type.__name__)
		yield tu.assert_db_type_creation, type


def test_db_types_conversion():
	from hermes.db.pgres.types import SUPPORTED

	for type in SUPPORTED:
		tu.assert_db_type_conversion.description = "tests.test_db_types.test_conversion :: {0}".format(type.__name__)
		yield tu.assert_db_type_conversion, type


def test_db_types_persistence():
	from hermes.db.pgres.types import SUPPORTED

	for type in SUPPORTED:
		tu.assert_db_type_persistence.description = "tests.test_db_types.test_persistence :: {0}".format(type.__name__)
		yield tu.assert_db_type_persistence, type
