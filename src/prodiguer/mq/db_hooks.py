# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.mq.db_hooks.py
   :copyright: Copyright "Apr 26, 2013", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Encapsulates hooks from MQ platform to database.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import sqlalchemy

import validation
from .. import db
from ..cv.factory import (
    create_activity,
    create_compute_node,
    create_compute_node_login,
    create_compute_node_machine,
    create_experiment,
    create_model,
    create_simulation_space,
    create_simulation_state
    )
from ..db.validation import (
    validate_activity,
    validate_compute_node,
    validate_compute_node_login,
    validate_compute_node_machine,
    validate_experiment,
    validate_model,
    validate_simulation_execution_start_date,
    validate_simulation_name,
    validate_simulation_output_end_date,
    validate_simulation_output_start_date,
    validate_simulation_space,
    validate_simulation_state,
    validate_simulation_state_info,
    validate_simulation_state_timestamp,
    validate_simulation_uid
    )
from ..utils import runtime as rt



def _get_id(cv_type, cv_term):
    """Utility function to map a CV name to an id.

    """
    return db.cache.get_id(cv_type, cv_term)


def _get_name(cv_type, cv_term):
    """Utility function to map a CV id to a name.

    """
    return db.cache.get_name(cv_type, cv_term)


def _validate_create_simulation_cv_terms(
    activity,
    compute_node,
    compute_node_login,
    compute_node_machine,
    execution_state,
    experiment,
    model,
    space
    ):
    """Validates set of cv terms creating them where necessary.

    """
    # Ensure that cv cache is loaded.
    db.cache.load()

    # Set of handlers (order matters).
    handlers = (
        (
            lambda: validate_activity(activity),
            lambda: create_activity(activity)
        ),
        (
            lambda: validate_compute_node(compute_node),
            lambda: create_compute_node(compute_node)
        ),
        (
            lambda: validate_compute_node_login(compute_node_login),
            lambda: create_compute_node_login(compute_node, compute_node_login)
        ),
        (
            lambda: validate_compute_node_machine(compute_node_machine),
            lambda: create_compute_node_machine(compute_node, compute_node_machine)
        ),
        (
            lambda: validate_experiment(experiment),
            lambda: create_experiment(activity, experiment)
        ),
        (
            lambda: validate_model(model),
            lambda: create_model(model)
        ),
        (
            lambda: validate_simulation_space(space),
            lambda: create_simulation_space(space)
        ),
        (
            lambda: validate_simulation_state(execution_state),
            lambda: create_simulation_state(execution_state)
        )
    )

    # Iteration 1: Validate & create new terms when invalid.
    to_revalidate = []
    for validator, factory in handlers:
        try:
            validator()
        except ValueError:
            try:
                db.session.add(factory())
            except sqlalchemy.exc.IntegrityError:
                db.session.rollback()
            finally:
                to_revalidate.append(validator)

    # If a term was added, reload cache and re-validate.
    if to_revalidate:
        db.cache.reload()
        for validator in to_revalidate:
            validator()


def _validate_create_simulation(
    activity,
    compute_node,
    compute_node_login,
    compute_node_machine,
    execution_start_date,
    state,
    experiment,
    model,
    name,
    output_start_date,
    output_end_date,
    space,
    uid):
    """Validates create simulation inputs.

    """
    _validate_create_simulation_cv_terms(
        activity,
        compute_node,
        compute_node_login,
        compute_node_machine,
        state,
        experiment,
        model,
        space
    )
    validate_simulation_execution_start_date(execution_start_date)
    validate_simulation_name(name)
    validate_simulation_output_start_date(output_start_date)
    validate_simulation_output_end_date(output_end_date)
    validate_simulation_uid(uid)


def _validate_create_simulation_state(uid, state, timestamp, info):
    """Validates create simulation state inputs.

    """
    validate_simulation_uid(uid)
    validate_simulation_state(state)
    validate_simulation_state_timestamp(timestamp)
    validate_simulation_state_info(info)


def _validate_update_simulation_state(uid):
    """Validates update simulation state inputs.

    """
    validate_simulation_uid(uid)


def _validate_create_message(
    uid,
    app_id,
    producer_id,
    type_id,
    content,
    content_encoding,
    content_type,
    correlation_id,
    timestamp,
    timestamp_precision,
    timestamp_raw):
    """Validates create message inputs.

    """
    validation.validate_uid(uid)
    validation.validate_app_id(app_id)
    validation.validate_producer_id(producer_id)
    validation.validate_type_id(type_id)
    if correlation_id:
        validation.validate_correlation_id(correlation_id)
    validation.validate_content(content, content_encoding, content_type)
    validation.validate_timestamp(timestamp, timestamp_precision, timestamp_raw)


def retrieve_simulation(uid):
    """Retrieves simulation details from db.

    :param str uid: UID of simulation.

    :returns: Simulation details.
    :rtype: db.types.cnode.simulation.Simulation

    """
    qfilter = db.types.Simulation.uid == unicode(uid)

    return db.dao.get_by_facet(db.types.Simulation, qfilter=qfilter)


def create_simulation(
    activity,
    compute_node,
    compute_node_login,
    compute_node_machine,
    execution_start_date,
    state,
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
    :param str state: State of simulation execution, e.g. COMPLETE.
    :param str experiment: Name of experiment, e.g. piControl.
    :param str model: Name of model, e.g. IPSLCM5A.
    :param str name: Name of simulation, e.g. v3.aqua4K.
    :param datetime output_start_date: Output start date.
    :param datetime output_end_date: Output start date.
    :param str space: Name of space, e.g. PROD.
    :param str uid: Simulation unique identifier.

    :returns: Newly created simulation.
    :rtype: db.types.Simulation

    """
    # Validate inputs.
    _validate_create_simulation(
        activity,
        compute_node,
        compute_node_login,
        compute_node_machine,
        execution_start_date,
        state,
        experiment,
        model,
        name,
        output_start_date,
        output_end_date,
        space,
        uid)

    # Instantiate.
    sim = db.types.Simulation()
    sim.activity_id = _get_id(db.types.Activity, activity)
    sim.compute_node_id = _get_id(db.types.ComputeNode, compute_node)
    sim.compute_node_login_id = _get_id(db.types.ComputeNodeLogin, compute_node_login)
    sim.compute_node_machine_id = _get_id(db.types.ComputeNodeMachine, compute_node_machine)
    sim.execution_start_date = execution_start_date
    sim.execution_state_id = _get_id(db.types.SimulationState, state)
    sim.experiment_id = _get_id(db.types.Experiment, experiment)
    sim.model_id = _get_id(db.types.Model, model)
    sim.name = unicode(name)
    sim.output_start_date = output_start_date
    sim.output_end_date = output_end_date
    sim.space_id = _get_id(db.types.SimulationSpace, space)
    sim.uid = unicode(uid)

    # Push to db.
    db.session.add(sim)
    rt.log_db("Created simulation: {0}.".format(uid))

    return sim


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
    instance = db.types.SimulationStateChange()
    instance.info = info
    instance.simulation_uid = unicode(uid)
    instance.state_id = _get_id(db.types.SimulationState, state)
    instance.timestamp = timestamp

    # Push to db.
    db.session.add(instance)
    msg = "Persisted simulation state to db :: {0} | {1}"
    msg = msg.format(uid, state)
    rt.log_db(msg)

    # Update current state.
    _update_simulation_state(uid)

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
    change = db.dao.get_latest_simulation_state_change(uid)
    if not change:
        return

    # Update simulation state.
    simulation.execution_state_id = change.state_id

    # Push to db.
    db.session.update(simulation)

    state_name = _get_name(db.types.SimulationState, change.state_id)
    msg = "Updated current simulation state :: {0} --> {1}".format(uid, state_name)
    rt.log_db(msg)


def create_message(
    uid,
    app_id,
    producer_id,
    type_id,
    content,
    content_encoding='utf-8',
    content_type='application/json',
    correlation_id=None,
    timestamp=None,
    timestamp_precision=None,
    timestamp_raw=None):
    """Creates a new related message record in db.

    :param str uid: Message unique identifer.
    :param str app_id: Message application id, e.g. smon.
    :param str producer_id: Message producer id, e.g. libigcm.
    :param str type_id: Message type id, e.g. 1001000.
    :param str name: Message content.
    :param str content_encoding: Message content encoding, e.g. utf-8.
    :param str content_type: Message content type, e.g. application/json.
    :param datetime.datetime timestamp: Message timestamp.
    :param str timestamp_precision: Message timestamp precision (ns | ms).
    :param str timestamp_raw: Message raw timestamp.

    :returns: Newly created message.
    :rtype: db.types.Message

    """
    # Validate inputs.
    _validate_create_message(
        uid,
        app_id,
        producer_id,
        type_id,
        content,
        content_encoding,
        content_type,
        correlation_id,
        timestamp,
        timestamp_precision,
        timestamp_raw
        )

    # Ensure that cache is loaded.
    db.cache.load()

    # Instantiate instance.
    msg = db.types.Message()
    msg.app_id = _get_id(db.types.MessageApplication, app_id)
    msg.content = content
    msg.content_encoding = content_encoding
    msg.content_type = content_type
    msg.correlation_id = correlation_id
    msg.producer_id = _get_id(db.types.MessageProducer, producer_id)
    if timestamp is not None:
        msg.timestamp = timestamp
    if timestamp_precision is not None:
        msg.timestamp_precision = timestamp_precision
    if timestamp_raw is not None:
        msg.timestamp_raw = timestamp_raw
    msg.type_id = _get_id(db.types.MessageType, type_id)
    msg.uid = uid

    # Push to db.
    db.session.add(msg)

    return msg
