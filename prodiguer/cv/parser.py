# -*- coding: utf-8 -*-

"""
.. module:: cv.parser.py
   :copyright: Copyright "December 01, 2014", IPSL
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Controlled vocabulary parser.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from hermes.cv import accessor as ta
from hermes.cv import cache
from hermes.cv import constants
from hermes.cv import formatter as tf
from hermes.cv import validator



def parse_term_type(term_type):
    """Parses a controlled vocabulary term type.

    :param str term_type: Type of CV term being parsed, e.g. experiment.

    :returns: Parsed cv term type.
    :rtype: str

    """
    validator.validate_term_type(term_type)

    return tf.format_term_type(term_type)


def parse_term_name(term_type, term_name, must_exist=True):
    """Parses a controlled vocabulary term.

    :param str term_type: Type of CV term being parsed, e.g. experiment.
    :param str term_name: Name of CV term being parsed, e.g. picontrol.
    :param bool must_exist: Flag indicating whether the name should exist or not.

    :returns: Parsed cv term name.
    :rtype: str

    """
    validator.validate_term_type(term_type)
    if must_exist:
        validator.validate_term_name(term_type, term_name)

    term_name = tf.format_term_name(term_name)
    for term in cache.get_termset(term_type):
        if term_name == ta.get_name(term):
            return term_name
        elif term_name in tf.format_synonyms(ta.get_synonyms(term)):
            return ta.get_name(term)

    return term_name


def parse_term_display_name(term_type, term_name, must_exist=True):
    """Returns a parsed term display name.

    :param str term_type: Type of CV term being parsed, e.g. experiment.
    :param str term_name: Name of CV term being parsed, e.g. picontrol.
    :param bool must_exist: Flag indicating whether the name should exist or not.

    :returns: Parsed cv term display name.
    :rtype: str

    """
    term_type = parse_term_type(term_type)
    if term_type in constants.CASE_SENSITIVE_TERM_TYPESET:
        return unicode(term_name)
    else:
        return parse_term_name(term_type, term_name, must_exist)


def parse_term_data(term_data):
    """Returns parsed term data.

    :param dict term_data: Data associated with a term.

    :returns: Parsed cv term data.
    :rtype: dict

    """
    validator.validate_term_data(term_data)

    return tf.format_term_data(term_data)
