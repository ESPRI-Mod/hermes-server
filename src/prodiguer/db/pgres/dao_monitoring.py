# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.dao_monitoring.py
   :copyright: Copyright "Apr 26, 2013", IPSL
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Monitoring data access operations.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import datetime

from prodiguer.db.pgres import dao, types, session
from prodiguer.cv import validation as cv_validator
from prodiguer.cv import constants as cv_constants
from prodiguer.db.pgres import validation as db_validator
from prodiguer.utils import rt


def _log(msg, force=False):
    """Logging helper function.

    """
    if force:
        rt.log_db(msg)


def _validate_create_simulation(
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
    uid):
    """Validates create simulation inputs.

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


def _validate_create_simulation_configuration(uid, card):
    """Validates create simulation configuration inputs.

    """
    db_validator.validate_simulation_uid(uid)
    db_validator.validate_simulation_configuration_card(card)


def _validate_create_simulation_state(uid, state, timestamp, info):
    """Validates create simulation state inputs.

    """
    cv_validator.validate_simulation_state(state)
    db_validator.validate_simulation_state_info(info)
    db_validator.validate_simulation_state_timestamp(timestamp)
    db_validator.validate_simulation_uid(uid)


def _validate_create_job(
    simulation_uid,
    job_uid,
    execution_start_date,
    expected_completion_delay
    ):
    """Validates create job inputs.

    """
    db_validator.validate_job_uid(job_uid)
    db_validator.validate_simulation_uid(simulation_uid)
    db_validator.validate_execution_start_date(execution_start_date)
    db_validator.validate_expected_completion_delay(expected_completion_delay)


def _validate_update_job_status(
    job_uid,
    timestamp,
    new_state
    ):
    """Validates update job status inputs.

    """
    db_validator.validate_job_uid(job_uid)
    db_validator.validate_execution_end_date(timestamp)
    cv_validator.validate_job_state(new_state)


def retrieve_simulation(uid):
    """Retrieves simulation details from db.

    :param str uid: UID of simulation.

    :returns: Simulation details.
    :rtype: types.monitoring.Simulation

    """
    qfilter = types.Simulation.uid == unicode(uid)

    return dao.get_by_facet(types.Simulation, qfilter=qfilter)


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


def retrieve_simulation_states(uid):
    """Retrieves simulation states from db.

    :param str uid: UID of simulation.

    :returns: Simulation states.
    :rtype: types.monitoring.SimulationState

    """
    return dao.get_by_facet(
        types.SimulationState,
        types.SimulationState.simulation_uid==unicode(uid),
        types.SimulationState.timestamp.desc(),
        True)


def create_simulation(
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
    uid):
    """Creates a new simulation record in db.

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

    :returns: Newly created simulation.
    :rtype: types.Simulation

    """
    # Validate inputs.
    _validate_create_simulation(
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
        uid)

    # Instantiate.
    sim = types.Simulation()
    sim.activity = unicode(activity)
    sim.compute_node = unicode(compute_node)
    sim.compute_node_login = unicode(compute_node_login)
    sim.compute_node_machine = unicode(compute_node_machine)
    sim.execution_start_date = execution_start_date
    sim.experiment = unicode(experiment)
    sim.model = unicode(model)
    sim.name = unicode(name)
    sim.output_start_date = output_start_date
    sim.output_end_date = output_end_date
    sim.space = unicode(space)
    sim.uid = unicode(uid)

    # Set hash id.
    sim.hashid = sim.get_hashid()

    # Push to db.
    session.add(sim)

    # Log.
    _log("Created simulation: {0}.".format(uid))

    return sim


def create_simulation_configuration(uid, card):
    """Creates a new simulation configuration db record.

    :param str uid: Simulation UID.
    :param str card: Simulation configuration card.

    """
    # Validate inputs.
    _validate_create_simulation_configuration(uid, card)

    # Instantiate instance.
    instance = types.SimulationConfiguration()
    instance.simulation_uid = unicode(uid)
    instance.card = unicode(card)

    # Push to db.
    session.add(instance)

    # Log.
    _log("Created simulation configuration: {0}.".format(uid))

    return instance


def create_simulation_state(uid, state, timestamp, info):
    """Creates a new simulation state record in db.

    :param str uid: Simulation UID.
    :param str state: Simulation execution state, e.g. COMPLETE.
    :param datetime.datetime timestamp: Simulation state update timestamp.
    :param str info: Short contextual description of state change.

    """
    # Validate inputs.
    _validate_create_simulation_state(uid, state, timestamp, info)

    # Instantiate instance.
    instance = types.SimulationState()
    instance.info = unicode(info)
    instance.simulation_uid = unicode(uid)
    instance.state = unicode(state)
    instance.timestamp = timestamp

    # Push to db.
    session.add(instance)

    # Log.
    msg = "Persisted simulation state to db :: {0} | {1}"
    msg = msg.format(uid, state)
    _log(msg)

    return instance


def create_job(
    simulation_uid,
    job_uid,
    execution_start_date,
    expected_completion_delay
    ):
    """Creates a new job record in db.

    :param str simulation_uid: Simulation UID.
    :param str job_uid: Job UID.
    :param datetime execution_start_date: Simulation start date.
    :param int expected_completion_delay: Delay before job completion is considered to be late.

    """
    # Validate inputs.
    _validate_create_job(
        simulation_uid,
        job_uid,
        execution_start_date,
        expected_completion_delay
        )

    # Instantiate instance.
    instance = types.Job()
    instance.job_uid = unicode(job_uid)
    instance.simulation_uid = unicode(simulation_uid)
    instance.execution_start_date = execution_start_date
    instance.execution_state = cv_constants.JOB_STATE_RUNNING
    instance.expected_execution_end_date = \
        execution_start_date + datetime.timedelta(seconds=int(expected_completion_delay))

    # Push to db.
    session.add(instance)

    # Log.
    msg = "Persisted job to db :: {0} | {1}"
    msg = msg.format(simulation_uid, job_uid)
    _log(msg)

    return instance


def update_job_status(
    job_uid,
    timestamp,
    new_state
    ):
    """Updates the status of a job record in db.

    :param str job_uid: Job UID.
    :param datetime timestamp: Status update timestamp.
    :param str new_state: New exeution status (COMPLETE | ERROR).

    """
    # Validate inputs.
    _validate_update_job_status(
        job_uid,
        timestamp,
        new_state
        )

    # Retrieve job.
    instance = retrieve_job(job_uid)
    if not instance or instance.execution_state == new_state:
        return None

    # Update job.
    instance.execution_end_date = timestamp
    instance.execution_state = new_state
    instance.was_late = timestamp > instance.expected_execution_end_date
    session.update(instance)

    # Log.
    msg = "Updated job state :: {0} | {1}"
    msg = msg.format(job_uid, new_state)
    _log(msg)

    return instance


def delete_dead_simulation_runs(hashid, uid):
    """Deletes so-called simulations dead runs (i.e. simulations that were rerun).

    :param str uid: Simulation UID of new simulation.
    :param str hashid: Simulation hash identifier.

    """
    # Get dead simulations.
    qry = session.query(types.Simulation)
    qry = qry.filter(types.Simulation.hashid == hashid)
    qry = qry.filter(types.Simulation.uid != uid)
    dead = qry.all()
    if not dead:
        return

    # Delete.
    for simulation in dead:
        delete_simulation(simulation.uid)
        _log("Deleting dead simulation :: {}".format(simulation.uid))


def delete_simulation(uid):
    """Deletes a simulation from database.

    """
    for etype in [
        types.Job,
        types.SimulationConfiguration,
        types.SimulationForcing,
        types.SimulationState
        ]:
        dao.delete_by_facet(etype, etype.simulation_uid == uid)
    dao.delete_by_facet(types.Message, types.Message.correlation_id_1 == uid)
    dao.delete_by_facet(types.Simulation, types.Simulation.uid == uid)
