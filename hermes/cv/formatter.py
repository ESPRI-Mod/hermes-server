# -*- coding: utf-8 -*-

"""
.. module:: cv.formatter.py
   :copyright: Copyright "December 01, 2014", IPSL
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Controlled vocabulary formatter.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""



def format_term_type(term_type):
    """Formats a controlled vocabulary term type.

    :param str term_type: Type of CV term being formatted, e.g. experiment.

    :returns: Formatted cv term type.
    :rtype: str

    """
    return unicode(term_type).lower()


def format_term_name(term_name):
    """Formats a controlled vocabulary term.

    :param str term_name: Name of CV term being formatted, e.g. picontrol.

    :returns: Formatted cv term name.
    :rtype: str

    """
    return unicode(term_name.lower())


def format_term_data(term_data):
    """Formats data associated with a controlled vocabulary term.

    :param dict term_data: Data associated with a CV term.

    :returns: Formatted cv term data.
    :rtype: dict

    """
    return term_data or {}


def format_synonyms(term_synonyms):
    """Formats a set of controlled vocabulary synonyms.

    :param list term_synonyms: Set of controlled vocabulary synonyms.

    :returns: Formatted set of synonyms.
    :rtype: list

    """
    return [unicode(s).strip().lower() for s in term_synonyms if s and len(s.strip())]
