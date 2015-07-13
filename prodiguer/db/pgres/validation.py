# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.validation.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Database related validation functions.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>

"""
import uuid

import arrow


def _raise_value_error(val, var, var_type):
    """Raises a generic value error.

    """
    raise ValueError('{0} [{1}] is an invalid {2}'.format(var, val, var_type))


def validate_bool(val, var):
    """Validates a boolean.

    """
    if val is None:
        raise ValueError('{0} is undefined bool'.format(var))

    try:
        bool(val)
    except ValueError:
        _raise_value_error(val, var, bool)


def validate_int(val, var):
    """Validates an integer.

    """
    if val is None:
        raise ValueError('{0} is undefined'.format(var))

    try:
        int(val)
    except ValueError:
        _raise_value_error(val, var, int)


def validate_date(val, var):
    """Validates a date.

    """
    if val is None:
        raise ValueError('{0} is undefined date'.format(var))

    try:
        arrow.get(val)
    except arrow.parser.ParserError:
        _raise_value_error(val, var, 'date')


def validate_uid(val, var):
    """Validaes a universally unique identifier.

    """
    if not isinstance(val, uuid.UUID):
        try:
            uuid.UUID(val)
        except ValueError:
            _raise_value_error(val, var, uuid.UUID)


def validate_accounting_project(project):
    """Validates an accounting project.

    :param str project: An accounting project.

    """
    pass


def validate_simulation_configuration_card(card):
    """Validate simulation configuration.

    :param str card: A simulation config card.

    """
    if card is None or not len(str(card).strip()):
        raise ValueError("Simulation configuration card is empty.")


def validate_execution_start_date(date):
    """Validate an execution start date.

    :param date: An execution start date.
    :type date: str | datetime.datetime

    """
    validate_date(date, 'Execution start date')


def validate_execution_end_date(date):
    """Validate an execution end date.

    :param date: An execution end date.
    :type date: str | datetime.datetime

    """
    validate_date(date, 'Execution end date')


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
    validate_date(date, 'Output start date')


def validate_simulation_output_end_date(date):
    """Validate simulation output end date.

    :param date: A simulation output end date.
    :type date: str | datetime.datetime

    """
    validate_date(date, 'Output end date')


def validate_simulation_uid(identifier):
    """Validates a simulation unique identifier.

    :param str identifier: A simulation unique identifier.

    """
    validate_uid(identifier, "Simulation uid")


def validate_simulation_hashid(identifier):
    """Validates a simulation hash id.

    :param str identifier: A simulation hash identifier.

    """
    pass


def validate_raw_activity(name):
    """Validates a raw activity name.

    :param str name: A raw activity name.

    """
    pass


def validate_raw_compute_node(name):
    """Validates a raw compute node name.

    :param str name: A raw compute node name.

    """
    pass



def validate_raw_compute_node_login(name):
    """Validates a raw compute node login name.

    :param str name: A raw compute node login name.

    """
    pass


def validate_raw_compute_node_machine(name):
    """Validates a raw compute node machine name.

    :param str name: A raw compute node machine name.

    """
    pass


def validate_raw_experiment(name):
    """Validates a raw experiment name.

    :param str name: A raw experiment name.

    """
    pass


def validate_raw_model(name):
    """Validates a raw model name.

    :param str name: A raw model name.

    """
    pass


def validate_raw_simulation_space(name):
    """Validates a raw simulation space name.

    :param str name: A raw simulation space name.

    """
    pass


def validate_job_uid(identifier):
    """Validates a job unique identifier.

    :param str identifier: A job unique identifier.

    """
    validate_uid(identifier, "Job uid")


def validate_job_state_timestamp(date):
    """Validate job state timestamp.

    :param date: A job state timestamp.
    :type date: str | datetime.datetime

    """
    validate_date(date, 'Job state timestamp')


def validate_job_state_info(info):
    """Validate job state info.

    :param str info: A job state information.

    """
    if info is None or len(info.strip()) == 0:
        raise TypeError('Job state info is undefined')


def validate_expected_completion_delay(delay):
    """Validates an expected completion transition delay time step.

    :param int delay: Number of seconds before a completion warning needs to be raised.

    """
    if delay:
        validate_int(delay, "Expected completion delay")
