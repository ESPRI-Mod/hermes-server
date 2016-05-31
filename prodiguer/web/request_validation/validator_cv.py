# -*- coding: utf-8 -*-

"""
.. module:: hermes.web.request_validation.validator_cv.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Validates controlled vocabulary endpoint requests.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer.web.request_validation import validator as rv



def validate_fetch(handler):
    """Validates fetch_cv endpoint HTTP request.

    """
    rv.validate(handler)

