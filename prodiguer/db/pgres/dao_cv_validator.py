# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.dao_cv_validator.py
   :copyright: Copyright "Apr 26, 2013", IPSL
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: CV data access validation operations.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer.cv import validation as cv_validator



def validate_create_term(
    term_type,
    term_name,
    term_display_name,
    ):
    """Function input validator: create_term.

    """
    cv_validator.validate_term_type(term_type)
    cv_validator.validate_term_name(term_type, term_name)
    cv_validator.validate_term_display_name(term_display_name)


def validate_retrieve_term(term_type, term_name):
    """Function input validator: retrieve_term.

    """
    pass


def validate_reset_terms():
    """Function input validator: reset_terms.

    """
    pass

