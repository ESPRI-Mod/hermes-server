# -*- coding: utf-8 -*-
"""
.. module:: hermes.cv.__init__.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Controlled vocabulary package initializer.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from hermes.cv import accessor
from hermes.cv import cache
from hermes.cv import exceptions
from hermes.cv import factory
from hermes.cv import io
from hermes.cv import parser
from hermes.cv import session
from hermes.cv import validator
from hermes.cv.accessor import get_create_date
from hermes.cv.accessor import get_description
from hermes.cv.accessor import get_display_name
from hermes.cv.accessor import get_name
from hermes.cv.accessor import get_synonyms
from hermes.cv.accessor import get_type
from hermes.cv.accessor import get_uid
from hermes.cv.accessor import get_sort_key
from hermes.cv.constants import *
from hermes.cv.factory import create
from hermes.cv.exceptions import TermNameError
from hermes.cv.exceptions import TermTypeError
from hermes.cv.exceptions import TermUserDataError
