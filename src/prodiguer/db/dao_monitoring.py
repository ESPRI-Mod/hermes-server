# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.dao_monitoring.py
   :copyright: Copyright "Apr 26, 2013", IPSL
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Monitoring data access operations.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer.db import dao, types, session
from prodiguer.cv.validation import (
    validate_activity,
    validate_compute_node,
    validate_compute_node_login,
    validate_compute_node_machine,
    validate_experiment,
    validate_model,
    validate_simulation_space,
    validate_simulation_state
    )
from prodiguer.db.validation import (
    validate_job_uid,
    validate_simulation_configuration_card,
    validate_simulation_execution_start_date,
    validate_simulation_name,
    validate_simulation_output_end_date,
    validate_simulation_output_start_date,
    validate_simulation_state_info,
    validate_simulation_state_timestamp,
    validate_simulation_uid
    )
from prodiguer.utils import rt



def _validate_create_simulation(
    activity,
    compute_node,
    compute_node_login,
    compute_node_machine,
    execution_start_date,
    execution_state,
    experiment,
    model,
    name,
    output_start_date,
    output_end_date,
    space,
    uid):
    """Validates create simulation inputs.

    """
    # Validate cv terms.
    validate_activity(activity)
    validate_compute_node(compute_node)
    validate_compute_node_login(compute_node_login)
    validate_compute_node_machine(compute_node_machine)
    validate_experiment(experiment)
    validate_model(model)
    validate_simulation_space(space)
    validate_simulation_state(execution_state)

    # Validate other fields.
    validate_simulation_execution_start_date(execution_start_date)
    validate_simulation_name(name)
    validate_simulation_output_start_date(output_start_date)
    validate_simulation_output_end_date(output_end_date)
    validate_simulation_uid(uid)


def _validate_create_simulation_configuration(uid, card):
    """Validates create simulation configuration inputs.

    """
    validate_simulation_uid(uid)
    validate_simulation_configuration_card(card)


def _validate_create_simulation_state(uid, state, timestamp, info):
    """Validates create simulation state inputs.

    """
    validate_simulation_uid(uid)
    validate_simulation_state(state)
    validate_simulation_state_timestamp(timestamp)
    validate_simulation_state_info(info)


def _validate_create_job_state(simulation_uid, job_uid, state, timestamp, info):
    """Validates create job state inputs.

    """
    validate_simulation_uid(simulation_uid)
    validate_job_uid(job_uid)
    validate_simulation_state(state)
    validate_simulation_state_timestamp(timestamp)
    validate_simulation_state_info(info)


def _validate_update_simulation_state(uid):
    """Validates update simulation state inputs.

    """
    validate_simulation_uid(uid)


def retrieve_simulation(uid):
    """Retrieves simulation details from db.

    :param str uid: UID of simulation.

    :returns: Simulation details.
    :rtype: types.monitoring.Simulation

    """
    qfilter = types.Simulation.uid == unicode(uid)

    return dao.get_by_facet(types.Simulation, qfilter=qfilter)


def get_latest_simulation_state_change(uid):
    """Returns latest simulation state change entry.

    :param str uid: Simulation unique identifier.

    :returns: Most recent simulation state change entry.
    :rtype: types.SimulationStateChange

    """
    return dao.get_by_facet(
        types.SimulationStateChange,
        types.SimulationStateChange.simulation_uid==unicode(uid),
        types.SimulationStateChange.timestamp.desc(),
        False)


def create_simulation(
    activity,
    compute_node,
    compute_node_login,
    compute_node_machine,
    execution_start_date,
    execution_state,
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
    :param str execution_state: State of simulation execution, e.g. COMPLETE.
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
        execution_state,
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
    sim.execution_state = unicode(execution_state)
    sim.experiment = unicode(experiment)
    sim.model = unicode(model)
    sim.name = unicode(name)
    sim.output_start_date = output_start_date
    sim.output_end_date = output_end_date
    sim.space = unicode(space)
    sim.uid = unicode(uid)

    # Push to db.
    session.add(sim)
    rt.log_db("Created simulation: {0}.".format(uid))

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
    rt.log_db("Created simulation configuration: {0}.".format(uid))

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
    instance = types.SimulationStateChange()
    instance.info = unicode(info)
    instance.simulation_uid = unicode(uid)
    instance.state = unicode(state)
    instance.timestamp = timestamp

    # Push to db.
    session.add(instance)
    msg = "Persisted simulation state to db :: {0} | {1}"
    msg = msg.format(uid, state)
    rt.log_db(msg)

    # Update state on simulation table.
    _update_simulation_state(uid)

    return instance


def create_job_state(simulation_uid, job_uid, state, timestamp, info):
    """Creates a new job state record in db.

    :param str simulation_uid: Simulation UID.
    :param str job_uid: Job UID.
    :param str state: Execution state, e.g. COMPLETE.
    :param datetime.datetime timestamp: State timestamp.
    :param str info: Short contextual description of state change.

    """
    # Validate inputs.
    _validate_create_job_state(simulation_uid, job_uid, state, timestamp, info)

    # Instantiate instance.
    instance = types.SimulationStateChange()
    instance.info = unicode(info)
    instance.job_uid = unicode(job_uid)
    instance.simulation_uid = unicode(simulation_uid)
    instance.state = unicode(state)
    instance.timestamp = timestamp

    # Push to db.
    session.add(instance)
    msg = "Persisted job state to db :: {0} | {1} | {2}"
    msg = msg.format(simulation_uid, job_uid, state)
    rt.log_db(msg)

    return instance


def _update_simulation_state(uid):
    """Updates simulation state.

    :param str uid: Simulation UID.

    """
    # Validate inputs.
    _validate_update_simulation_state(uid)

    # Get simulation.
    simulation = retrieve_simulation(uid)
    if not simulation:
        return

    # Get latest state change.
    change = get_latest_simulation_state_change(uid)
    if not change:
        return

    # Update simulation state.
    simulation.execution_state = change.state

    # Push to db.
    session.update(simulation)
    msg = "Updated current simulation state :: {0} --> {1}".format(uid, change.state)
    rt.log_db(msg)
