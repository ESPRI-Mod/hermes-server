# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.dao_monitoring.py
   :copyright: Copyright "Apr 26, 2013", IPSL
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Monitoring data access operations.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from sqlalchemy.exc import IntegrityError

import datetime

from prodiguer.db.pgres import dao
from prodiguer.db.pgres import session
from prodiguer.db.pgres import types
from prodiguer.cv import validation as cv_validator
from prodiguer.db.pgres import validation as db_validator
from prodiguer.utils import decorators



def _persist(hydrate, create, retrieve):
    """Persists to db by either creating a new instance or
       retrieving and updating an existing instance.

    """
    try:
        instance = create()
        hydrate(instance)
        session.insert(instance)
    except IntegrityError:
        session.rollback()
        instance = retrieve()
        hydrate(instance)
        session.update(instance)

    return instance


def _validate_persist_simulation_01(
    activity,
    compute_node,
    compute_node_login,
    compute_node_machine,
    execution_start_date,
    experiment,
    model,
    name,
    output_start_date,
    output_end_date,
    space,
    uid
    ):
    """Validates persist simulation inputs.

    """
    cv_validator.validate_activity(activity)
    cv_validator.validate_compute_node(compute_node)
    cv_validator.validate_compute_node_login(compute_node_login)
    cv_validator.validate_compute_node_machine(compute_node_machine)
    cv_validator.validate_experiment(experiment)
    cv_validator.validate_model(model)
    cv_validator.validate_simulation_space(space)
    db_validator.validate_execution_start_date(execution_start_date)
    db_validator.validate_simulation_name(name)
    db_validator.validate_simulation_output_start_date(output_start_date)
    db_validator.validate_simulation_output_end_date(output_end_date)
    db_validator.validate_simulation_uid(uid)


def _validate_persist_simulation_02(execution_end_date, is_error, uid):
    """Validates persist simulation inputs.

    """
    db_validator.validate_bool(is_error, 'Is Error flag')
    db_validator.validate_execution_end_date(execution_end_date)
    db_validator.validate_simulation_uid(uid)


def _validate_persist_simulation_configuration(uid, card):
    """Validates create simulation configuration inputs.

    """
    db_validator.validate_simulation_uid(uid)
    db_validator.validate_simulation_configuration_card(card)


def _validate_persist_job_01(
    expected_completion_delay,
    execution_start_date,
    job_uid,
    simulation_uid
    ):
    """Validates persist job inputs.

    """
    db_validator.validate_expected_completion_delay(expected_completion_delay)
    db_validator.validate_execution_start_date(execution_start_date)
    db_validator.validate_job_uid(job_uid)
    db_validator.validate_simulation_uid(simulation_uid)


def _validate_persist_job_02(
    execution_end_date,
    is_error,
    job_uid,
    simulation_uid
    ):
    """Validates persist job inputs.

    """
    db_validator.validate_execution_end_date(execution_end_date)
    db_validator.validate_bool(is_error, 'Is Error flag')
    db_validator.validate_job_uid(job_uid)
    db_validator.validate_simulation_uid(simulation_uid)


def retrieve_active_simulations():
    """Retrieves active simulation details from db.

    :returns: Simulation details.
    :rtype: list

    """
    qry = session.query(types.Simulation)
    qry = qry.filter(types.Simulation.name != None)
    qry = qry.filter(types.Simulation.is_obsolete == False)

    return dao.exec_query(types.Simulation, qry, True)


def retrieve_active_simulation(hashid):
    """Retrieves an active simulation from db.

    :param str hashid: Simulation hash identifier.

    :returns: An active simulation instance.
    :rtype: types.Simulation

    """
    qry = session.query(types.Simulation)
    qry = qry.filter(types.Simulation.hashid == hashid)
    qry = qry.filter(types.Simulation.is_obsolete == False)

    return dao.exec_query(types.Simulation, qry)


def retrieve_active_jobs():
    """Retrieves active job details from db.

    :returns: Job details.
    :rtype: list

    """
    qry = session.query(types.Job)
    qry = qry.join(types.Simulation,
                   types.Job.simulation_uid==types.Simulation.uid)
    qry = qry.filter(types.Simulation.is_obsolete == False)

    return dao.exec_query(types.Job, qry, True)


def retrieve_simulation(uid):
    """Retrieves simulation details from db.

    :param str uid: UID of simulation.

    :returns: Simulation details.
    :rtype: types.monitoring.Simulation

    """
    qfilter = types.Simulation.uid == unicode(uid)

    return dao.get_by_facet(types.Simulation, qfilter=qfilter)


def retrieve_simulation_configuration(uid):
    """Retrieves simulation configuration details from db.

    :param str uid: UID of simulation.

    :returns: Simulation configuration details.
    :rtype: types.monitoring.SimulationConfiguration

    """
    qfilter = types.SimulationConfiguration.simulation_uid == unicode(uid)

    return dao.get_by_facet(types.SimulationConfiguration, qfilter=qfilter)


def retrieve_simulation_jobs(uid):
    """Retrieves job details from db.

    :param str uid: UID of simulation.

    :returns: List of jobs associated with a simulation.
    :rtype: types.monitoring.Job

    """
    qfilter = types.Job.simulation_uid == unicode(uid)

    return dao.get_by_facet(types.Job, qfilter=qfilter, get_iterable=True)


def retrieve_job(uid):
    """Retrieves job details from db.

    :param str uid: UID of job.

    :returns: Job details.
    :rtype: types.monitoring.Job

    """
    qfilter = types.Job.job_uid == unicode(uid)

    return dao.get_by_facet(types.Job, qfilter=qfilter)


@decorators.validate(_validate_persist_simulation_01)
def persist_simulation_01(
    activity,
    compute_node,
    compute_node_login,
    compute_node_machine,
    execution_start_date,
    experiment,
    model,
    name,
    output_start_date,
    output_end_date,
    space,
    uid
    ):
    """Persists simulation information to db.

    :param str activity: Name of activity, e.g. IPSL.
    :param str compute_node: Name of compute node, e.g. TGCC.
    :param str compute_node_login: Name of compute node login, e.g. dcugnet.
    :param str compute_node_machine: Name of compute machine, e.g. SX9.
    :param datetime execution_start_date: Simulation start date.
    :param str experiment: Name of experiment, e.g. piControl.
    :param str model: Name of model, e.g. IPSLCM5A.
    :param str name: Name of simulation, e.g. v3.aqua4K.
    :param datetime output_start_date: Output start date.
    :param datetime output_end_date: Output start date.
    :param str space: Name of space, e.g. PROD.
    :param str uid: Simulation unique identifier.

    :returns: Either a new or an updated simulation instance.
    :rtype: types.Simulation

    """
    def _assign(instance):
        """Assigns instance values from input parameters.

        """
        instance.activity = unicode(activity)
        instance.compute_node = unicode(compute_node)
        instance.compute_node_login = unicode(compute_node_login)
        instance.compute_node_machine = unicode(compute_node_machine)
        instance.execution_start_date = execution_start_date
        instance.experiment = unicode(experiment)
        instance.model = unicode(model)
        instance.name = unicode(name)
        instance.output_start_date = output_start_date
        instance.output_end_date = output_end_date
        instance.space = unicode(space)
        instance.uid = unicode(uid)
        instance.hashid = instance.get_hashid()

    return _persist(_assign, types.Simulation, lambda: retrieve_simulation(uid))


@decorators.validate(_validate_persist_simulation_02)
def persist_simulation_02(execution_end_date, is_error, uid):
    """Persists simulation information to db.

    :param datetime execution_end_date: Simulation end date.
    :param bool is_error: Flag indicating whether the simulation terminated in error.
    :param str uid: Simulation unique identifier.

    :returns: Either a new or an updated simulation instance.
    :rtype: types.Simulation

    """
    def _assign(instance):
        """Assigns instance values from input parameters.

        """
        instance.execution_end_date = execution_end_date
        instance.is_error = is_error
        instance.uid = unicode(uid)

    return _persist(_assign, types.Simulation, lambda: retrieve_simulation(uid))


@decorators.validate(_validate_persist_simulation_configuration)
def persist_simulation_configuration(uid, card):
    """Persists a new simulation configuration db record.

    :param str uid: Simulation UID.
    :param str card: Simulation configuration card.

    """
    # Instantiate instance.
    instance = types.SimulationConfiguration()
    instance.simulation_uid = unicode(uid)
    instance.card = unicode(card)

    # Push to db.
    session.add(instance)

    return instance


@decorators.validate(_validate_persist_job_01)
def persist_job_01(
    expected_completion_delay,
    execution_start_date,
    job_uid,
    simulation_uid
    ):
    """Persists job information to db.

    :param int expected_completion_delay: Delay before job completion is considered to be late.
    :param datetime execution_start_date: Simulation start date.
    :param str job_uid: Job UID.
    :param str simulation_uid: Simulation UID.

    :returns: Either a new or an updated job instance.
    :rtype: types.Job

    """
    def _assign(instance):
        """Assigns instance values from input parameters.

        """
        instance.execution_start_date = execution_start_date
        instance.expected_execution_end_date = \
            execution_start_date + \
            datetime.timedelta(seconds=int(expected_completion_delay))
        instance.job_uid = unicode(job_uid)
        instance.simulation_uid = unicode(simulation_uid)
        instance.set_was_late_flag()

    return _persist(_assign, types.Job, lambda: retrieve_job(job_uid))


@decorators.validate(_validate_persist_job_02)
def persist_job_02(execution_end_date, is_error, job_uid, simulation_uid):
    """Persists job information to db.

    :param datetime execution_end_date: Job end date.
    :param bool is_error: Flag indicating whether the job terminated in error.
    :param str job_uid: Job unique identifier.
    :param str simulation_uid: Simulation UID.

    :returns: Either a new or an updated job instance.
    :rtype: types.Job

    """
    def _assign(instance):
        """Assigns instance values from input parameters.

        """
        instance.execution_end_date = execution_end_date
        instance.is_error = is_error
        instance.job_uid = unicode(job_uid)
        instance.simulation_uid = unicode(simulation_uid)
        instance.set_was_late_flag()

    return _persist(_assign, types.Job, lambda: retrieve_job(job_uid))


def update_active_simulation(hashid):
    """Updates the active simulation within a group.

    :param str hashid: A simulation hash identifier used to group a batch of simulations.

    """
    # Set simulation group.
    qry = session.query(types.Simulation)
    qry = qry.filter(types.Simulation.hashid == hashid)
    group = sorted(qry.all(), key=lambda s: s.execution_start_date)

    # Update try identifier & obsolete flag.
    for index, simulation in enumerate(group, 1):
        simulation.try_id = index
        simulation.is_obsolete = (index != len(group))

    # Return active.
    return group[-1]


def delete_simulation(uid):
    """Deletes a simulation from database.

    """
    for etype in [
        types.Job,
        types.SimulationConfiguration
        ]:
        dao.delete_by_facet(etype, etype.simulation_uid == uid)
    dao.delete_by_facet(types.Message, types.Message.correlation_id_1 == uid)
    dao.delete_by_facet(types.Simulation, types.Simulation.uid == uid)
