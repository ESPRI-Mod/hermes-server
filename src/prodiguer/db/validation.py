# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.validation.py
   :copyright: Copyright "May 21, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Database related validation functions.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>

"""
import uuid

import arrow

import cache, types


def validate_cv_term(cv_type, term_id):
    """Validates a cv term.

    """
    cv_type = unicode(cv_type).lower()
    term_id = unicode(term_id).lower()

    for term in cache.get_collection(types.CvTerm):
        if term.cv_type != cv_type:
            continue
        if term.name.lower() == term_id:
            return
        if term.synonyms:
            names = term.synonyms.split(",")
            names = [n.strip().lower() for n in names if n and n.strip()]
            if cv_type == "model":
                print "validating synonym", term_id, names, term_id in names
            if term_id in names:
                print "Validated cv term against synonyms"
                return

    raise ValueError('Unknown cv term: {0}.{1}'.format(cv_type, term_id))


def _validate_date(date, var):
    """Validates a date.

    """
    if date is None:
        raise ValueError('{0} is undefined'.format(var))

    try:
        arrow.get(date)
    except arrow.parser.ParserError:
        raise ValueError('{0} is invalid'.format(var))


def _validate_uid(identifier, var):
    """Validaes a universally unique identifier.

    """
    if not isinstance(identifier, uuid.UUID):
        try:
            uuid.UUID(identifier)
        except ValueError:
            raise ValueError("{0} must be UUID compatible.".format(var))


def validate_activity(identifier):
    """Validate activity identifier.

    :param identifier: An activity identifier.
    :type identifier: str | int

    """
    validate_cv_term("activity", identifier)


def validate_compute_node(identifier):
    """Validate compute node identifier.

    :param identifier: A compute node identifier.
    :type identifier: str | int

    """
    validate_cv_term("compute_node", identifier)


def validate_compute_node_login(identifier):
    """Validate compute node login identifier.

    :param identifier: A compute node login identifier.
    :type identifier: str | int

    """
    validate_cv_term("compute_node_login", identifier)


def validate_compute_node_machine(identifier):
    """Validate compute node machine identifier.

    :param identifier: A compute node machine identifier.
    :type identifier: str | int

    """
    validate_cv_term("compute_node_machine", identifier)


def validate_experiment(identifier):
    """Validate experiment identifier.

    :param identifier: An experiment identifier.
    :type identifier: str | int

    """
    validate_cv_term("experiment", identifier)


def validate_model(identifier):
    """Validate model identifier.

    :param identifier: A model identifier.
    :type identifier: str | int

    """
    validate_cv_term("model", identifier)


def validate_job_uid(identifier):
    """Validates a simulation job unique identifier.

    :param str identifier: A simulation job unique identifier.

    """
    _validate_uid(identifier, "Job uid")


def validate_simulation_execution_start_date(date):
    """Validate simulation execution start date.

    :param date: A simulation execution start date.
    :type date: str | datetime.datetime

    """
    _validate_date(date, 'Execution start date')


def validate_simulation_state_timestamp(date):
    """Validate simulation state timestamp.

    :param date: A simulation state timestamp.
    :type date: str | datetime.datetime

    """
    _validate_date(date, 'State timestamp')


def validate_simulation_state_info(info):
    """Validate simulation state info.

    :param str info: A simulation state information.

    """
    if info is None or len(info.strip()) == 0:
        raise TypeError('Info is undefined')


def validate_simulation_name(name):
    """Validate simulation name.

    :param str name: A simulation name.

    """
    if name is None or len(name.strip()) == 0:
        raise TypeError('Name is undefined')


def validate_simulation_output_start_date(date):
    """Validate simulation output start date.

    :param date: A simulation output start date.
    :type date: str | datetime.datetime

    """
    _validate_date(date, 'Output start date')


def validate_simulation_output_end_date(date):
    """Validate simulation output end date.

    :param date: A simulation output end date.
    :type date: str | datetime.datetime

    """
    _validate_date(date, 'Output end date')


def validate_simulation_space(identifier):
    """Validate simulation space identifier.

    :param identifier: A simulation space identifier.
    :type identifier: str | int

    """
    validate_cv_term("simulation_space", identifier)


def validate_simulation_state(identifier):
    """Validate simulation state identifier.

    :param identifier: A simulation state identifier.
    :type identifier: str | int

    """
    validate_cv_term("simulation_state", identifier)


def validate_simulation_uid(identifier):
    """Validates a simulation unique identifier.

    :param str identifier: A simulation unique identifier.

    """
    _validate_uid(identifier, "Simulation uid")

