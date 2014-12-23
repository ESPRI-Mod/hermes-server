# -*- coding: utf-8 -*-

"""
.. module:: cv.validator.py
   :copyright: Copyright "December 01, 2014", IPSL
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Controlled vocabulary validator.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer.cv import constants, cache, term_accessor as ta



def _is_matching_name(term, term_name):
    """Returns a name by matching against a term's sysnonyms.

    """
    if ta.get_name(term) == term_name:
        return True


def _is_matching_synonym(term, term_name):
    """Returns a name by matching against a term's sysnonyms.

    """
    return term_name in ta.get_synonyms(term)


# Term name matching predicate functions.
_NAME_MATCHING_PREDICATES = (
    _is_matching_name,
    _is_matching_synonym
    )


def validate_term_name(term_type, term_name):
    """Validates a term name.

    """
    # Format inputs.
    term_name = unicode(term_name).lower()
    term_type = unicode(term_type).lower()

    # Validate term set.
    validate_term_type(term_type)

    # Match term by name or synonyms.
    for term in cache.get_termset(term_type):
        for predicate in _NAME_MATCHING_PREDICATES:
            if predicate(term, term_name):
                return ta.get_name(term)

    # Term was unmatched therefore error.
    err = "Unknown cv term: {0}.{1}".format(term_type, term_name)
    raise ValueError(err)


def validate_term_type(term_type):
    """Validates that a term type is supported.

    """
    term_type = unicode(term_type).lower()
    if term_type not in constants.TERM_TYPESET:
        raise ValueError("Unknown CV type :{}".format(term_type))


def validate_activity(term_name):
    """Validate activity term name.

    :param str term_name: An activity term name.

    """
    validate_term_name(constants.TERM_TYPE_ACTIVITY, term_name)


def validate_compute_node(term_name):
    """Validate compute node term name.

    :param str term_name: A compute node term name.

    """
    validate_term_name(constants.TERM_TYPE_COMPUTE_NODE, term_name)


def validate_compute_node_login(term_name):
    """Validate compute node login term name.

    :param str term_name: A compute node login term name.

    """
    validate_term_name(constants.TERM_TYPE_COMPUTE_NODE_LOGIN, term_name)


def validate_compute_node_machine(term_name):
    """Validate compute node machine term name.

    :param str term_name: A compute node machine term name.

    """
    validate_term_name(constants.TERM_TYPE_COMPUTE_NODE_MACHINE, term_name)


def validate_experiment(term_name):
    """Validate experiment term name.

    :param str term_name: An experiment term name.

    """
    validate_term_name(constants.TERM_TYPE_EXPERIMENT, term_name)


def validate_message_application(term_name):
    """Validate MQ platform message application.

    :param str term_name: A message application name.

    """
    validate_term_name(constants.TERM_TYPE_MESSAGE_APPLICATION, term_name)


def validate_message_producer(term_name):
    """Validate MQ platform message producer.

    :param str term_name: A message producer name.

    """
    validate_term_name(constants.TERM_TYPE_MESSAGE_PRODUCER, term_name)


def validate_message_type(term_name):
    """Validate MQ platform message type.

    :param str term_name: A message type name.

    """
    validate_term_name(constants.TERM_TYPE_MESSAGE_TYPE, term_name)


def validate_message_user(term_name):
    """Validate MQ platform message user.

    :param str term_name: A message user name.

    """
    validate_term_name(constants.TERM_TYPE_MESSAGE_USER, term_name)


def validate_model(term_name):
    """Validate model term name.

    :param str term_name: A model term name.

    """
    validate_term_name(constants.TERM_TYPE_MODEL, term_name)


def validate_simulation_space(term_name):
    """Validate simulation space term name.

    :param str term_name: A simulation space term name.
    :type term_name: str

    """
    validate_term_name(constants.TERM_TYPE_SIMULATION_SPACE, term_name)


def validate_simulation_state(term_name):
    """Validate simulation state term name.

    :param str term_name: A simulation state term name.
    :type term_name: str

    """
    validate_term_name(constants.TERM_TYPE_SIMULATION_STATE, term_name)
