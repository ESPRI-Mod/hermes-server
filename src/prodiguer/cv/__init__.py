# -*- coding: utf-8 -*-
"""
.. module:: prodiguer.cv.__init__.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Controlled vocabulary package initializer.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer.cv import (
	cache,
	exceptions,
	factory,
	io,
	parser,
	session,
	accessor,
	validation
	)
from accessor import (
	get_display_name,
	get_name,
	get_synonyms,
	get_type
	)
from factory import create
from exceptions import TermNameError, TermTypeError, TermUserDataError
