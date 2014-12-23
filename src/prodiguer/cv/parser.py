# -*- coding: utf-8 -*-

"""
.. module:: cv.parser.py
   :copyright: Copyright "December 01, 2014", IPSL
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Controlled vocabulary parser.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer.cv import validation



def parse_term_name(term_type, term_name):
    """Parses a controlled vocabulary term.

    :param str term_type: Type of CV term being parsed, e.g. activity.
    :param str term_name: Name of CV term being parsed, e.g. ipsl.

    :returns: Parsed cv term name.
    :rtype: unicode

    """
    # Delegate to validator as this returns parsed term name.
    return validation.validate_term_name(term_type, term_name)
