# -*- coding: utf-8 -*-

"""
.. module:: test_mq.py

   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL / CeCILL
   :platform: Unix
   :synopsis: Encapsulates mq tests.

.. moduleauthor:: IPSL (ES-DOC) <dev@esdocumentation.org>

"""
import datetime
import inspect

import nose

from . import utils as tu
from prodiguer import cv
from prodiguer.db.types import Message
from prodiguer.db.types import Simulation


# Test constants.
_SIM_ACTIVITY = 'IPSL'
_SIM_COMPUTE_NODE = 'CCRT'
_SIM_COMPUTE_NODE_LOGIN = 'dcugnet'
_SIM_COMPUTE_NODE_MACHINE = 'CCRT - SX9'
_SIM_EXECUTION_START_DATE = datetime.datetime.now()
_SIM_EXECUTION_STATE = cv.constants.SIMULATION_STATE_RUNNING
_SIM_EXPERIMENT = '1pctCO2'
_SIM_MODEL_ENGINE = 'IPSL-CM5A-LR'
_SIM_SPACE = cv.constants.SIMULATION_SPACE_TEST
_MSG_APP = "smon"
_MSG_PUBLISHER = "libigcm"
_MSG_TYPE = "1000000"
_MSG_CONTENT1 = "12345690"
_MSG_CONTENT2 = "12345690"



def _create_simulation(name=tu.get_string(63)):
    import prodiguer.db.dao_monitoring as db_hooks

    s = db_hooks.create_simulation(_SIM_ACTIVITY,
                                   _SIM_COMPUTE_NODE,
                                   _SIM_COMPUTE_NODE_LOGIN,
                                   _SIM_COMPUTE_NODE_MACHINE,
                                   _SIM_EXECUTION_START_DATE,
                                   _SIM_EXECUTION_STATE,
                                   _SIM_EXPERIMENT,
                                   _SIM_MODEL_ENGINE,
                                   name,
                                   _SIM_SPACE,
                                   parent_simulation_name=None)
    tu.assert_obj(s, Simulation)
    tu.assert_obj(s.id, int)
    tu.assert_string(s.name, name)
    tu.assert_date(s.execution_start_date, str(_SIM_EXECUTION_START_DATE))
    tu.assert_collection(db_hooks.retrieve_messages(name), 0)

    return s


def _update_simulation_state(name, state):
    import prodiguer.db.db_hooks as db_hooks

    db_hooks.update_simulation_status(name, state)

    s = db_hooks.retrieve_simulation(name)
    tu.assert_obj(s, Simulation)
    # tu.assert_obj(db.dao.get_by_id(CvTerm, s.execution_state), CvTerm)


def _delete_simulation(name):
    import prodiguer.db.dao_mq as db_hooks

    db_hooks.delete_simulation(name)
    s = db_hooks.retrieve_simulation(name)
    tu.assert_none(s)


def _create_simulation_message(s):
    import prodiguer.db.dao_mq as db_hooks
    
    m = db_hooks.create_simulation_message(s.name,
                                           _MSG_APP,
                                           _MSG_PUBLISHER,
                                           _MSG_TYPE,
                                           _MSG_CONTENT1)
    tu.assert_obj(m, Message)

    return m


def _create_simulation_messages(s, n=2):               
    return tuple([_create_simulation_message(s) for i in range(n)])



@nose.tools.nottest
def test_imports():
    import prodiguer.db.db_hooks as db_hooks
    tu.assert_bool(inspect.ismodule(db_hooks))


@nose.tools.nottest
def test_simulation_cycle():
    import prodiguer.db.db_hooks as db_hooks

    # Create.
    s1 = _create_simulation()

    # Retrieve.
    s2 = db_hooks.retrieve_simulation(s1.name)
    tu.assert_obj(s2, Simulation)
    tu.assert_integer(s2.id, s1.id)

    # Update status.
    for state in cv.constants.SIMULATION_STATE_SET:
        _update_simulation_state(s1.name, state)

    # Delete.
    _delete_simulation(s1.name)


def test_simulation_message_cycle():
    import prodiguer.db.dao_mq as db_hooks

    # Create simulation.
    s1 = _create_simulation()

    # Create messages.
    m1, m2 = _create_simulation_messages(s1)

    # Load messages.
    m_list = db_hooks.retrieve_messages(s1.name)
    tu.assert_collection(m_list, 2, Message)
    assert m1 in m_list
    assert m2 in m_list

    # Get last message.
    m_last = db_hooks.retrieve_last_message(s1.name)
    tu.assert_obj(m_last, Message)
    assert m_last == m2

    # Delete messages.
    db_hooks.delete_messages(s1.name)
    m_list = db_hooks.retrieve_messages(s1.name)
    tu.assert_collection(m_list, 0)

    # Delete simulation.
    db_hooks.delete_simulation(s1.name)
    tu.assert_none(db_hooks.retrieve_simulation(s1.name))    

