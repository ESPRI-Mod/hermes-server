# -*- coding: utf-8 -*-

"""
.. module:: test_cv.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Encapsulates cv tests.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from os.path import dirname, abspath, exists, join

from sqlalchemy import inspect

from prodiguer import cv
from prodiguer.db import pgres as db
from . import utils as tu



def test_cv_get_filename():
	pass
	# tu.assert_string(cv.get_filename(db.types.CvTerm), "shared_cv_term.json")


def test_cv_get_filepath():
	folder = dirname(abspath(__file__))
	folder = dirname(folder)
	folder = join(folder, "src")
	folder = join(folder, "prodiguer")
	folder = join(folder, "cv")
	folder = join(folder, "json")
	# for type in (db.types.CvTerm, ):
	# 	fp = join(folder, cv.get_filename(type))
	# 	tu.assert_bool(exists(fp))
	# 	tu.assert_string(cv.get_filepath(type), fp)


def test_cv_list_types():
	schemas = []
	types = []
	for s, t in cv.list_types():
		types.append(t)
		if not s in schemas:
			schemas.append(s)

	tu.assert_integer(len(schemas), 4)
	tu.assert_integer(len(types), 22)


def test_cv_read():
	pass
	# for type in (db.types.CvTerm, ):
	# 	for d in cv.read(type):
	# 		tu.assert_obj(d, dict)
	# 		for c in inspect(type).columns:
	# 			if c.name not in ['row_create_date', 'row_update_date']:
	# 				assert c.name in d, c.name
	# 				if d[c.name] is not None:
	# 					assert c.type.python_type == d[c.name].__class__
	# 				else:
	# 					assert c.nullable == True


def test_cv_load():
	pass
	# for type in (db.types.CvTerm, ):
	# 	for i in cv.load(type):
	# 		tu.assert_obj(i, type)
