# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.web.__init__.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: API package initializer.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer.web.app import run
from prodiguer.web import utils_handler as handler_utils
from prodiguer.web import utils_ws as ws_utils
