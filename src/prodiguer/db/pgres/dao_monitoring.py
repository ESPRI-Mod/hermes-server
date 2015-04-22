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


def retrieve_active_simulations():
    """Retrieves active simulation details from db.

    :returns: Simulation details.
    :rtype: list

    """
    qfilter = types.Simulation.is_dead == False

    return dao.get_by_facet(types.Simulation,
                            qfilter=qfilter,
                            get_iterable=True)


def retrieve_active_jobs():
    """Retrieves active job details from db.

    :returns: Job details.
    :rtype: list

    """
    qry = session.query(types.Job)
    qry = qry.join(types.Simulation,
                   types.Job.simulation_uid==types.Simulation.uid)
    qry = qry.filter(types.Simulation.is_dead == False)

    return dao.sort(types.Job, qry.all())


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
    instance.expected_execution_end_date = \
        execution_start_date + datetime.timedelta(seconds=int(expected_completion_delay))

    # Push to db.
    session.add(instance)

    # Log.
    msg = "Persisted job to db :: {0} | {1}"
    msg = msg.format(simulation_uid, job_uid)
    _log(msg)

    return instance


def update_dead_simulation_runs(hashid, uid):
    """Updates so-called simulations dead runs (i.e. simulations that were rerun).

    :param str uid: Simulation UID of new simulation.
    :param str hashid: Simulation hash identifier.

    """
    # Get dead simulations.
    qry = session.query(types.Simulation)
    qry = qry.filter(types.Simulation.hashid == hashid)
    qry = qry.filter(types.Simulation.uid != uid)
    qry = qry.filter(types.Simulation.is_dead == False)

    # Update is_dead flag.
    for simulation in qry.all():
        simulation.is_dead = True
        _log("Updating dead simulation :: {}".format(simulation.uid))


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
