# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.__init__.py

   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL / CeCILL
   :platform: Unix
   :synopsis: Top level package intializer.

.. moduleauthor:: IPSL (ES-DOC) <dev@esdocumentation.org>

"""
__version__ = '0.2.0.0'


from os.path import dirname, abspath, join

from prodiguer import api, cv, db, mq, utils
from prodiguer.utils import (
	config,
	convert,
	data_convertor,
	mail,
	rt,
	string_convertor
	)


# Module exports.
__all__ = [
	'api',
	'config',
	'convert',
	'cv',
	'db',
	'mq',
	'utils',
]

