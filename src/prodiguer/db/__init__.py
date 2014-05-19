# -*- coding: utf-8 -*-
"""
.. module:: prodiguer.db.__init__.py
   :copyright: Copyright "Jun 12, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Database package initializer.

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>


"""
# Module imports.
from . import (
	cache,
	constants,
	dao,
	dao_metrics,
	mq_hooks,
	session,
	setup,
	types,
	type_factory
	)
from ..utils import config



# Module exports.
__all__ = [
	"cache",
	"constants",
	"dao",
	"dao_metrics",
	"mq_hooks",
	"initialize",
	"session",
	"setup",
	"types",
	"type_factory"
]



def initialize():
	"""Initialises sub-package.

	"""
	if config.db.auto_initialize:	
		session.start(config.db.connection)

