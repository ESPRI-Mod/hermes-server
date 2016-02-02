# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.__init__.py

   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL / CeCILL
   :platform: Unix
   :synopsis: Top level package intializer.

.. moduleauthor:: IPSL (ES-DOC) <dev@esdocumentation.org>

"""
__version__ = '0.4.4.0'


from prodiguer import cv
from prodiguer import db
from prodiguer import mq
from prodiguer import utils
from prodiguer import web
from prodiguer.cv import VOCAB_DOMAIN
from prodiguer.cv import VOCAB_SUBDOMAIN
from prodiguer.utils import config
from prodiguer.utils import mail
