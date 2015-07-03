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
    accounting_project,
    activity,
    activity_raw,
    compute_node,
    compute_node_raw,
    compute_node_login,
    compute_node_login_raw,
    compute_node_machine,
    compute_node_machine_raw,
    execution_start_date,
    experiment,
    experiment_raw,
    model,
    model_raw,
    name,
    output_start_date,
    output_end_date,
    space,
    space_raw,
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
    db_validator.validate_accounting_project(accounting_project)
    db_validator.validate_execution_start_date(execution_start_date)
    db_validator.validate_raw_activity(activity_raw)
    db_validator.validate_raw_compute_node(compute_node_raw)
    db_validator.validate_raw_compute_node_login(compute_node_login_raw)
    db_validator.validate_raw_compute_node_machine(compute_node_machine_raw)
    db_validator.validate_raw_experiment(experiment_raw)
    db_validator.validate_raw_model(model_raw)
    db_validator.validate_raw_simulation_space(space_raw)
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
    accounting_project,
    expected_completion_delay,
    execution_start_date,
    typeof,
    job_uid,
    simulation_uid
    ):
    """Validates persist job inputs.

    """
    db_validator.validate_accounting_project(accounting_project)
    db_validator.validate_expected_completion_delay(expected_completion_delay)
    db_validator.validate_execution_start_date(execution_start_date)
    cv_validator.validate_job_type(typeof)
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


def exists(uid):
    """Returns a flag indicating whether simulation already exists.

    :param str uid: UID of simulation.

    :returns: True if simulation exists false otherwise.
    :rtype: bool

    """
    qfilter = types.Simulation.uid == unicode(uid)

    return dao.get_count(types.Simulation, qfilter=qfilter) == 1


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

    return dao.get_by_facet(types.Job, qfilter=qfilter, get_iterable=True,
                            order_by=types.Job.execution_start_date.asc())


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
    accounting_project,
    activity,
    activity_raw,
    compute_node,
    compute_node_raw,
    compute_node_login,
    compute_node_login_raw,
    compute_node_machine,
    compute_node_machine_raw,
    execution_start_date,
    experiment,
    experiment_raw,
    model,
    model_raw,
    name,
    output_start_date,
    output_end_date,
    space,
    space_raw,
    uid
    ):
    """Persists simulation information to db.

    :param str accounting_project: Name of associated accounting project.
    :param str activity: Name of activity, e.g. IPSL.
    :param str activity_raw: Name of activity before CV reformatting.
    :param str compute_node: Name of compute node, e.g. TGCC.
    :param str compute_node_raw: Name of compute node before CV reformatting.
    :param str compute_node_login: Name of compute node login, e.g. dcugnet.
    :param str compute_node_login_raw: Name of compute node login before CV reformatting.
    :param str compute_node_machine: Name of compute machine, e.g. SX9.
    :param str compute_node_machine_raw: Name of compute node machine before CV reformatting.
    :param datetime execution_start_date: Simulation start date.
    :param str experiment: Name of experiment, e.g. piControl.
    :param str experiment_raw: Name of experiment before CV reformatting.
    :param str model: Name of model, e.g. IPSLCM5A.
    :param str model_raw: Name of model before CV reformatting.
    :param str name: Name of simulation, e.g. v3.aqua4K.
    :param datetime output_start_date: Output start date.
    :param datetime output_end_date: Output start date.
    :param str space: Name of space, e.g. PROD.
    :param str space_raw: Name of space before CV reformatting.
    :param str uid: Simulation unique identifier.

    :returns: Either a new or an updated simulation instance.
    :rtype: types.Simulation

    """
    def _assign(instance):
        """Assigns instance values from input parameters.

        """
        instance.accounting_project = unicode(accounting_project)
        instance.activity = unicode(activity)
        instance.activity_raw = unicode(activity_raw)
        instance.compute_node = unicode(compute_node)
        instance.compute_node_raw = unicode(compute_node_raw)
        instance.compute_node_login = unicode(compute_node_login)
        instance.compute_node_login_raw = unicode(compute_node_login_raw)
        instance.compute_node_machine = unicode(compute_node_machine)
        instance.compute_node_machine_raw = unicode(compute_node_machine_raw)
        instance.execution_start_date = execution_start_date
        instance.experiment = unicode(experiment)
        instance.experiment_raw = unicode(experiment_raw)
        instance.model = unicode(model)
        instance.model_raw = unicode(model_raw)
        instance.name = unicode(name)
        instance.output_start_date = output_start_date
        instance.output_end_date = output_end_date
        instance.space = unicode(space)
        instance.space_raw = unicode(space_raw)
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
    accounting_project,
    expected_completion_delay,
    execution_start_date,
    typeof,
    job_uid,
    simulation_uid
    ):
    """Persists job information to db.

    :param str accounting_project: Name of associated accounting project.
    :param int expected_completion_delay: Delay before job completion is considered to be late.
    :param datetime execution_start_date: Simulation start date.
    :param str typeof: Job type.
    :param str job_uid: Job UID.
    :param str simulation_uid: Simulation UID.

    :returns: Either a new or an updated job instance.
    :rtype: types.Job

    """
    def _assign(instance):
        """Assigns instance values from input parameters.

        """
        if accounting_project:
            instance.accounting_project = unicode(accounting_project)
        instance.execution_start_date = execution_start_date
        instance.typeof = unicode(typeof)
        instance.job_uid = unicode(job_uid)
        instance.simulation_uid = unicode(simulation_uid)
        if expected_completion_delay:
            instance.expected_execution_end_date = \
                execution_start_date + \
                datetime.timedelta(seconds=int(expected_completion_delay))
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


# @decorators.validate(_validate_persist_job_01)
def persist_environment_metric(
    action_name,
    action_timestamp,
    dir_from,
    dir_to,
    duration_ms,
    job_uid,
    simulation_uid,
    size_mb,
    throughput_mb_s
    ):
    """Persists environment metric information to db.

    :param str action_name: Name of libIGCM action.
    :param str dir_from: Directory from which data was copied.
    :param str dir_to: Directory to which data was copied.
    :param integer duration_ms: Duration in milliseconds of action.
    :param str job_uid: Job UID.
    :param str simulation_uid: Simulation UID.
    :param float size_mb: Size in megabytes of moved file(s).
    :param datetime timestamp: Time when action took place.
    :param float throughput_mb_s: Rate at which copy took place.

    :returns: A new environment metrics instance.
    :rtype: types.EnvironmentMetric

    """
    instance = types.EnvironmentMetric()
    instance.action_name = unicode(action_name)
    instance.action_timestamp = action_timestamp
    instance.dir_from = unicode(dir_from)
    instance.dir_to = unicode(dir_to)
    instance.duration_ms = duration_ms
    instance.job_uid = unicode(job_uid)
    instance.simulation_uid = unicode(simulation_uid)
    instance.size_mb = size_mb
    instance.throughput_mb_s = throughput_mb_s

    # Push to db.
    session.add(instance)

    return instance
