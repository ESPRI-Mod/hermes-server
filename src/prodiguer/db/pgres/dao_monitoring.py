# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.dao_monitoring.py
   :copyright: Copyright "Apr 26, 2013", IPSL
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Monitoring data access operations.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer.db.pgres import dao, types, session
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
from prodiguer.db.pgres.validation import (
    validate_expected_state_transition_delay,
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


def _validate_create_job_state(
    simulation_uid,
    job_uid,
    state,
    timestamp,
    info,
    expected_transition_delay=None
    ):
    """Validates create job state inputs.

    """
    validate_simulation_uid(simulation_uid)
    validate_job_uid(job_uid)
    validate_simulation_state(state)
    validate_simulation_state_timestamp(timestamp)
    validate_simulation_state_info(info)
    if expected_transition_delay:
        validate_expected_state_transition_delay(expected_transition_delay)


def retrieve_simulation(uid):
    """Retrieves simulation details from db.

    :param str uid: UID of simulation.

    :returns: Simulation details.
    :rtype: types.monitoring.Simulation

    """
    qfilter = types.Simulation.uid == unicode(uid)

    return dao.get_by_facet(types.Simulation, qfilter=qfilter)


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


def get_latest_simulation_state_change(uid):
    """Returns latest simulation state change entry.

    :param str uid: Simulation unique identifier.

    :returns: Most recent simulation state change entry.
    :rtype: types.SimulationState

    """
    return dao.get_by_facet(
        types.SimulationState,
        types.SimulationState.simulation_uid==unicode(uid),
        types.SimulationState.timestamp.desc(),
        False)


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
    instance = types.SimulationState()
    instance.info = unicode(info)
    instance.simulation_uid = unicode(uid)
    instance.state = unicode(state)
    instance.timestamp = timestamp

    # Push to db.
    session.add(instance)
    msg = "Persisted simulation state to db :: {0} | {1}"
    msg = msg.format(uid, state)
    rt.log_db(msg)

    return instance


def create_job_state(
    simulation_uid,
    job_uid,
    state,
    timestamp,
    info,
    expected_transition_delay=None
    ):
    """Creates a new job state record in db.

    :param str simulation_uid: Simulation UID.
    :param str job_uid: Job UID.
    :param str state: Execution state, e.g. COMPLETE.
    :param datetime.datetime timestamp: State timestamp.
    :param str info: Short contextual description of state change.

    """
    # Validate inputs.
    _validate_create_job_state(
        simulation_uid,
        job_uid, state,
        timestamp,
        info,
        expected_transition_delay
        )

    # Instantiate instance.
    instance = types.SimulationState()
    instance.info = unicode(info)
    instance.job_uid = unicode(job_uid)
    instance.simulation_uid = unicode(simulation_uid)
    instance.state = unicode(state)
    instance.timestamp = timestamp
    instance.expected_transition_delay = None if not expected_transition_delay else \
                                         int(expected_transition_delay)

    # Push to db.
    session.add(instance)
    msg = "Persisted job state to db :: {0} | {1} | {2}"
    msg = msg.format(simulation_uid, job_uid, state)
    rt.log_db(msg)

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

    # Log.
    msg = "Deleted dead simulations :: {}".format([i.uid for i in dead])
    rt.log_db(msg)


def delete_simulation(uid):
    """Deletes a simulation from database.

    """
    for etype in [
        types.SimulationConfiguration,
        types.SimulationForcing,
        types.SimulationState
        ]:
        dao.delete_by_facet(etype, etype.simulation_uid == uid)
    dao.delete_by_facet(types.Message, types.Message.correlation_id_1 == uid)
    dao.delete_by_facet(types.Simulation, types.Simulation.uid == uid)
