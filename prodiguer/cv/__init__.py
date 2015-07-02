# -*- coding: utf-8 -*-
"""
.. module:: prodiguer.cv.__init__.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Controlled vocabulary package initializer.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer.cv import accessor
from prodiguer.cv import cache
from prodiguer.cv import exceptions
from prodiguer.cv import factory
from prodiguer.cv import io
from prodiguer.cv import parser
from prodiguer.cv import session
from prodiguer.cv import validation
from prodiguer.cv.accessor import get_display_name
from prodiguer.cv.accessor import get_name
from prodiguer.cv.accessor import get_synonyms
from prodiguer.cv.accessor import get_type
from prodiguer.cv.factory import create
from prodiguer.cv.exceptions import TermNameError
from prodiguer.cv.exceptions import TermTypeError
from prodiguer.cv.exceptions import TermUserDataError
