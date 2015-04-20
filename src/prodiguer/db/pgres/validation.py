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



def _validate_int(val, var):
    """Validates an integer.

    """
    if val is None:
        raise ValueError('{0} is undefined'.format(var))

    try:
        int(val)
    except ValueError:
        raise ValueError('{0} is invalid'.format(var))


def _validate_date(val, var):
    """Validates a date.

    """
    if val is None:
        raise ValueError('{0} is undefined'.format(var))

    try:
        arrow.get(val)
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


def validate_simulation_job_uid(identifier):
    """Validates a simulation job unique identifier.

    :param str identifier: A simulation job unique identifier.

    """
    _validate_uid(identifier, "Job uid")


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
    _validate_date(date, 'Execution start date')


def validate_execution_end_date(date):
    """Validate an execution end date.

    :param date: An execution end date.
    :type date: str | datetime.datetime

    """
    _validate_date(date, 'Execution end date')


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


def validate_simulation_uid(identifier):
    """Validates a simulation unique identifier.

    :param str identifier: A simulation unique identifier.

    """
    _validate_uid(identifier, "Simulation uid")


def validate_job_uid(identifier):
    """Validates a job unique identifier.

    :param str identifier: A job unique identifier.

    """
    _validate_uid(identifier, "Job uid")


def validate_job_state_timestamp(date):
    """Validate job state timestamp.

    :param date: A job state timestamp.
    :type date: str | datetime.datetime

    """
    _validate_date(date, 'Job state timestamp')


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
        _validate_int(delay, "Expected completion delay")
