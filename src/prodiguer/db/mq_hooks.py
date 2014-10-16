# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.mq_hooks.py
   :copyright: Copyright "Apr 26, 2013", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Encapsulates hooks from message queues to database.

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>


"""
# Module imports.
import datetime

from dateutil import parser as date_parser

from . import cache, constants, dao, types as dbtypes
from ..utils import runtime as rt



def _get_id(type, name):
    """Utility function to map a name to an id."""
    return cache.get_id(type, name)


def retrieve_simulation_by_uid(uid):
    """Retrieves simulation details from db.

    :param str uid: UID of simulation.

    :returns: Simulation details.
    :rtype: dbtypes.cnode.simulation.Simulation

    """
    # rt.log_db("Retrieving simulation: {0}.".format(uid))

    qfilter = dbtypes.Simulation.uid == unicode(uid)

    return dao.get_by_facet(dbtypes.Simulation, qfilter=qfilter)


def _validate_cvs(activity,
                  compute_node,
                  compute_node_login,
                  compute_node_machine,
                  execution_state,
                  experiment,
                  model,
                  space):
    """Validates set of cv terms.

    """
    # Ensure that cache is loaded.
    cache.load()

    # Defensive programming.
    for key, item in (
        (dbtypes.Activity, activity),
        (dbtypes.ComputeNode, compute_node),
        (dbtypes.ComputeNodeLogin, compute_node_login),
        (dbtypes.ComputeNodeMachine, compute_node_machine),
        (dbtypes.SimulationState, execution_state),
        (dbtypes.Experiment, experiment),
        (dbtypes.Model, model),
        (dbtypes.SimulationSpace, space),
    ):
        if not cache.exists(key, item):
            raise ValueError('{0} is unknown'.format(key))


def delete_simulation(name):
    """Deletes a simulation from db.

    :param name: Name of simulation, e.g. v3.aqua4K.
    :type name: str

    """
    # rt.log_db("Deleting simulation.")

    dao.delete_by_name(dbtypes.Simulation, name)


def create_simulation(activity,
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
                      uid,
                      parent_simulation_name=None):
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
    :param str parent_simulation_name: Name of parent simulation, e.g. piControl2.

    :returns: Newly created simulation.
    :rtype: dbtypes.Simulation

    """
    # Validate controlled vocabularies.
    _validate_cvs(activity,
                  compute_node,
                  compute_node_login,
                  compute_node_machine,
                  execution_state,
                  experiment,
                  model,
                  space)

    # Validate other fields.
    if execution_start_date is None:
        raise TypeError('Execution start date is undefined')
    if not isinstance(execution_start_date, datetime.datetime):
        execution_start_date = date_parser.parse(execution_start_date)
        if not isinstance(execution_start_date, datetime.datetime):
            raise TypeError('Execution start date must be a valid date')
    if name is None:
        raise TypeError('Name is undefined')
    if output_start_date is None:
        raise TypeError('Output start date is undefined')
    if not isinstance(output_start_date, datetime.datetime):
        output_start_date = date_parser.parse(output_start_date)
        if not isinstance(output_start_date, datetime.datetime):
            raise TypeError('Output start date must be a valid date')
    if output_end_date is None:
        raise TypeError('Output end date is undefined')
    if not isinstance(output_end_date, datetime.datetime):
        output_end_date = date_parser.parse(output_end_date)
        if not isinstance(output_end_date, datetime.datetime):
            raise TypeError('Output end date must be a valid date')
    if uid is None:
        raise TypeError('UID is undefined')

    # Instantiate an instance.
    s = dbtypes.Simulation()
    s.activity_id = _get_id(dbtypes.Activity, activity)
    s.compute_node_id = _get_id(dbtypes.ComputeNode, compute_node)
    s.compute_node_login_id = _get_id(dbtypes.ComputeNodeLogin, compute_node_login)
    s.compute_node_machine_id = _get_id(dbtypes.ComputeNodeMachine, compute_node_machine)
    s.execution_start_date = execution_start_date
    s.execution_state_id = _get_id(dbtypes.SimulationState, execution_state)
    s.experiment_id = _get_id(dbtypes.Experiment, experiment)
    s.model_id = _get_id(dbtypes.Model, model)
    s.name = unicode(name)
    s.output_start_date = output_start_date
    s.output_end_date = output_end_date
    s.space_id = _get_id(dbtypes.SimulationSpace, space)
    s.uid = unicode(uid)

    # Append to session.
    dao.insert(s)

    rt.log_db("Created simulation: {0}.".format(uid))

    return s


def update_simulation_status(uid, execution_state):
    """Updates status of an existing simulation record in db.

    :param uid: UID of simulation.
    :type name: str

    :param execution_state: State of simulation execution, e.g. COMPLETE.
    :type execution_state: str

    """
    # Defensive programming.
    if uid is None:
        raise TypeError('Simulation UID is undefined')
    if not cache.exists(dbtypes.SimulationState, execution_state):
        raise ValueError('Execution state is unknown')

    # Log.
    msg = "Persisting simulation state update to db :: {0} | {1}"
    rt.log_db(msg.format(uid, execution_state))

    # Update status.
    simulation = retrieve_simulation_by_uid(uid)
    if simulation is not None:
        simulation.execution_state_id = \
            _get_id(dbtypes.SimulationState, execution_state)

    return simulation


def create_message(uid,
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
    :rtype: dbtypes.Message

    """
    def guard():
        if not cache.exists(dbtypes.MessageApplication, app_id):
            raise ValueError('Message application is unknown')
        if not cache.exists(dbtypes.MessageProducer, producer_id):
            raise ValueError('Message publisher is unknown')
        if not cache.exists(dbtypes.MessageType, type_id):
            raise ValueError('Message type is unknown: {0}'.format(type_id))
        if content is None:
            raise TypeError('Message content is empty')

    # Ensure that cache is loaded.
    cache.load()

    # Defensive programming.
    guard()

    # Instantiate a message instance.
    msg = dbtypes.Message()
    msg.app_id = _get_id(dbtypes.MessageApplication, app_id)
    msg.content = content
    msg.content_encoding = content_encoding
    msg.content_type = content_type
    msg.correlation_id = correlation_id
    msg.producer_id = _get_id(dbtypes.MessageProducer, producer_id)
    if timestamp is not None:
        msg.timestamp = timestamp
    if timestamp_precision is not None:
        msg.timestamp_precision = timestamp_precision
    if timestamp_raw is not None:
        msg.timestamp_raw = timestamp_raw
    msg.type_id = _get_id(dbtypes.MessageType, type_id)
    msg.uid = uid

    # Append to session.
    dao.insert(msg)

    return msg

