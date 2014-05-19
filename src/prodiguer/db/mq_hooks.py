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



def retrieve_simulations(state_id=constants.EXECUTION_STATE_RUNNING_ID):
    """Retrieves list of simulation details from db filtered by execution state.

    :param state_id: ID of execution state.
    :type state_id: int

    :returns: Simulation details.
    :rtype: dbtypes.cnode.simulation.Simulation

    """
    rt.log_mq("Retrieving simulations list filtered by execution state.")

    return dao.get_simulations_by_state(state_id)


def retrieve_simulation(name):
    """Retrieves simulation details from db.

    :param name: Name of simulation, e.g. v3.aqua4K.
    :type name: str

    :returns: Simulation details.
    :rtype: dbtypes.cnode.simulation.Simulation

    """
    rt.log_mq("Retrieving simulation.")

    return dao.get_by_name(dbtypes.Simulation, name)


def delete_simulation(name):
    """Deletes a simulation from db.

    :param name: Name of simulation, e.g. v3.aqua4K.
    :type name: str

    """
    rt.log_mq("Deleting simulation.")

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
                      space,
                      parent_simulation_name=None):
    """Creates a new simulation record in db.

    :param activity: Name of activity, e.g. IPSL.
    :type activity: str

    :param compute_node: Name of compute node, e.g. TGCC.
    :type compute_node: str

    :param compute_node_login: Name of compute node login, e.g. dcugnet.
    :type compute_node_login: str

    :param compute_node_machine: Name of compute machine, e.g. SX9.
    :type compute_node_machine: str

    :param execution_start_date: Simulation start date.
    :type execution_start_date: datetime

    :param execution_state: State of simulation execution, e.g. COMPLETE.
    :type execution_state: str
    
    :param experiment: Name of experiment, e.g. piControl.
    :type experiment: str

    :param model: Name of model, e.g. IPSLCM5A.
    :type model: str

    :param name: Name of simulation, e.g. v3.aqua4K.
    :type name: str

    :param space: Name of space, e.g. PROD.
    :type space: str

    :param parent_simulation_name: Name of parent simulation, e.g. piControl2.
    :type parent_simulation_name: str

    :returns: Newly created simulation.
    :rtype: dbtypes.Simulation
    
    """
    # Log.
    msg = "Persisting new simulation to db :: {0} | {1} | {2} | {3} | {4} | {5} | {6} | {7} | {8} | {9}"
    rt.log_mq(msg.format(name, activity, compute_node, 
                         compute_node_login, compute_node_machine, execution_start_date, 
                         execution_state, experiment, model, space))    

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
    if execution_start_date is None:
        raise TypeError('Execution start date is undefined')
    if not isinstance(execution_start_date, datetime.datetime):
        execution_start_date = date_parser.parse(execution_start_date)
        if not isinstance(execution_start_date, datetime.datetime):
            raise TypeError('Execution start date must be a valid date')
    if name is None:
        raise TypeError('Name is undefined')

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
    s.name = str(name)
    s.space_id = _get_id(dbtypes.SimulationSpace, space)

    # TODO process parent simulation name

    # Append to session.
    dao.insert(s)

    return s


def update_simulation_status(name, execution_state):
    """Updates status of an existing simulation record in db.

    :param name: Name of simulation, e.g. v3.aqua4K.
    :type name: str

    :param execution_state: State of simulation execution, e.g. COMPLETE.
    :type execution_state: str
    
    """
    def log():
        msg = "Persisting simulation state update to db :: {0} | {1}"
        rt.log_mq(msg.format(name, execution_state))    

    def guard():
        if name is None:
            raise TypeError('Name is undefined')
        if not cache.exists(dbtypes.SimulationState, execution_state):
            raise ValueError('Execution state is unknown')

    # Log.
    log()

    # Ensure that cache is loaded.
    cache.load()

    # Defensive programming.
    guard()

    # Update status.
    s = retrieve_simulation(name)
    if s is not None:
        s.execution_state_id = _get_id(dbtypes.SimulationState, execution_state)

    return s


def create_message(app_id, 
                   publisher_id, 
                   type_id,
                   content,
                   content_encoding='utf-8',
                   content_type='application/json',
                   uid=None,
                   timestamp=None):
    """Creates a new related message record in db.

    :param app_id: Message application id, e.g. smon.
    :type app_id: str

    :param publisher_id: Message application id, e.g. libligcm.
    :type publisher_id: str

    :param type_id: Message type id, e.g. 1001000.
    :type type_id: str

    :param name: Message content.
    :type name: str

    :param content_encoding: Message content encoding, e.g. utf-8.
    :type content_encoding: str

    :param content_type: Message content type, e.g. application/json.
    :type content_type: str

    :param uid: Message unique identifer.
    :type uid: str

    :param timestamp: Message timestamp in milliseconds since epoch.
    :type timestamp: str

    :returns: Newly created message.
    :rtype: dbtypes.Message

    """
    def guard():
        if not cache.exists(dbtypes.MessageApplication, app_id):
            raise ValueError('Message application is unknown')
        if not cache.exists(dbtypes.MessagePublisher, publisher_id):
            raise ValueError('Message publisher is unknown')
        if not cache.exists(dbtypes.MessageType, type_id):
            raise ValueError('Message type is unknown')
        if content is None:
            raise TypeError('Message content is empty')

    # Ensure that cache is loaded.
    cache.load()

    # Defensive programming.
    guard()

    # Instantiate a message instance.
    m = dbtypes.Message()
    m.app_id = _get_id(dbtypes.MessageApplication, app_id)
    m.publisher_id = _get_id(dbtypes.MessagePublisher, publisher_id)
    m.type_id = _get_id(dbtypes.MessageType, type_id)
    m.content = content
    m.content_encoding = content_encoding
    m.content_type = content_type
    if uid is not None:
        m.uid = uid
    if timestamp is not None:
        m.timestamp = timestamp

    # Append to session.
    dao.insert(m)

    return m


def create_simulation_message(name,
                              app_id, 
                              publisher_id, 
                              type_id,
                              content,
                              content_encoding='utf-8',
                              content_type='application/json',
                              uid=None,
                              timestamp=None):
    """Creates a new simulation related message record in db.

    :param name: Name of simulation for which a message has been received.
    :type name: str
    
    :param app_id: Message application id, e.g. smon.
    :type app_id: str

    :param publisher_id: Message application id, e.g. libligcm.
    :type publisher_id: str

    :param type_id: Message type id, e.g. 1001000.
    :type type_id: str

    :param name: Message content.
    :type name: str

    :param content_encoding: Message content encoding, e.g. utf-8.
    :type content_encoding: str

    :param content_type: Message content type, e.g. application/json.
    :type content_type: str

    :param uid: Message unique identifer.
    :type uid: str

    :param timestamp: Message timestamp in milliseconds since epoch.
    :type timestamp: str

    :returns: Newly created message.
    :rtype: dbtypes.Message

    """
    # Ensure that cache is loaded.
    cache.load()

    # Defensive programming.
    if name is None:
        raise TypeError('Simulation Name is undefined')        
    s = retrieve_simulation(name)
    if s is None:
        raise ValueError('Simulation name is unknown :: {0}'.format(name))

    # Instantiate a message instance.
    m = create_message(app_id, 
                       publisher_id, 
                       type_id, 
                       content, 
                       content_encoding, 
                       content_type, 
                       uid, 
                       timestamp)
    dao.insert(m)

    # Instantiate a simulation message instance.
    sm = dbtypes.SimulationMessage()
    sm.simulation_id = s.id
    sm.message_id = m.id
    dao.insert(sm)

    sm.message = m
    sm.simulation = s

    return m


def retrieve_messages(name):
    """Retrieves set of messages received for a simulation (used in failover scenarios).

    :param name: Name of simulation, e.g. v3.aqua4K.
    :type name: str

    :returns: List of received simulation messages.
    :rtype: list
    
    """
    return dao.get_message_by_simulation(name)


def retrieve_last_message(name):
    """Retrieves last messsage received for a simulation (used in failover scenarios).

    :param name: Name of simulation, e.g. v3.aqua4K.
    :type name: str

    :returns: Last received simulation message.
    :rtype: dbtypes.Message

    """
    return dao.get_message_by_simulation(name, get_last=True)


def delete_messages(name):
    """Deletes set of messages received for a simulation (used in failover scenarios).

    :param name: Name of simulation, e.g. v3.aqua4K.
    :type name: str

    :returns: List of received simulation messages.
    :rtype: list

    """
    dao.delete_message_by_simulation(name)
