# -*- coding: utf-8 -*-
"""
.. module:: prodiguer.cv.__init__.py
   :copyright: Copyright "Jun 12, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Controlled vocabulary package initializer.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from .io import (
	get_filename,
	get_filepath,
	list_types,
	load,
	read,
	write
	)
import cache, factory
from .factory import create
from .parser import parse
