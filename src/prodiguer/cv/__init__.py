# -*- coding: utf-8 -*-
"""
.. module:: prodiguer.cv.__init__.py
   :copyright: Copyright "Jun 12, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Controlled vocabulary package initializer.

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>


"""
# Module imports.
from .io import (
	get_filename,
	get_filepath,
	list_types,
	load,
	read,
	write
	)



# Module exports.
__all__ = [
	"get_filename",
	"get_filepath",
	"list_types",
	"load",
	"read",
	"write",
	"setup"
]