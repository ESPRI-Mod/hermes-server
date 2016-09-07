# -*- coding: utf-8 -*-

"""
.. module:: cv.validator.py
   :copyright: Copyright "December 01, 2014", IPSL
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Controlled vocabulary validator.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import uuid

from prodiguer.cv import constants
from prodiguer.cv import cache
from prodiguer.cv import exceptions
from prodiguer.cv import formatter as tf
from prodiguer.cv import accessor as ta



def _is_matching_name(term, term_name):
    """Returns a name by matching against a term's sysnonyms.

    """
    if ta.get_name(term) == term_name:
        return True


def _is_matching_synonym(term, term_name):
    """Returns a name by matching against a term's sysnonyms.

    """
    return term_name in tf.format_synonyms(ta.get_synonyms(term))


# Term name matching predicate functions.
_NAME_MATCHING_PREDICATES = (
    _is_matching_name,
    _is_matching_synonym
    )


def validate_term_name(term_type, term_name):
    """Validates a term name.

    """
    # Ensure cache is loaded.
    cache.load()

    # Validate term set.
    term_type = tf.format_term_type(term_type)
    validate_term_type(term_type)

    # Match term either by name or synonyms.
    term_name = tf.format_term_name(term_name)
    for term in cache.get_termset(term_type):
        for is_matched in _NAME_MATCHING_PREDICATES:
            if is_matched(term, term_name):
                return ta.get_name(term)

    # Term was unmatched therefore error.
    raise exceptions.TermNameError(term_type, term_name)


def validate_term_type(term_type):
    """Validates that a term type is supported.

    """
    # Ensure cache is loaded.
    cache.load()

    term_type = tf.format_term_type(term_type)
    if term_type not in constants.TERM_TYPESET:
        raise exceptions.TermTypeError(term_type)


def validate_term_display_name(term_display_name):
    """Validates a term's display name.

    """
    pass


def validate_term_uid(term_uid):
    """Validates a term's uid.

    """
    if not isinstance(term_uid, uuid.UUID):
        try:
            uuid.UUID(term_uid)
        except ValueError:
            raise exceptions.TermUIDError(term_uid)


def validate_term_data(term_data):
    """Validates data associated with a term.

    """
    term_data = tf.format_term_data(term_data)
    if not isinstance(term_data, dict) or 'meta' in term_data:
        raise exceptions.TermUserDataError()


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


def validate_job_type(term_name):
    """Validate job type.

    :param str term_name: A job type.

    """
    validate_term_name(constants.TERM_TYPE_JOB_TYPE, term_name)


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


def validate_job_state(term_name):
    """Validate jon state term name.

    :param str term_name: A job state term name.
    :type term_name: str

    """
    validate_term_name(constants.TERM_TYPE_SIMULATION_STATE, term_name)
