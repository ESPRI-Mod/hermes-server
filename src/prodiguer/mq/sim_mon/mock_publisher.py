# -*- coding: utf-8 -*-
# Module imports.
import datetime
import random
import uuid

from .. import constants, utils
from ... import db


# List of simulations.
_s_list = None


def _get_name(type):
	"""Returns a random CV name from db."""
	return db.cache.get_random_name(type)


def _get_sim_list():
	"""Returns a mock list of simulations."""
	global _s_list

	if _s_list is None:
		_s_list = [s.name for s in db.dao.get_all(db.types.Simulation) if s.name.startswith('v3')]

	return _s_list


def _get_sim_state():
	sim_states = db.constants.EXECUTION_STATE_SET
	i = random.randint(0, len(sim_states) - 1)

	return sim_states[i]


def _get_sim_name():	
	"""Returns a random simulation name."""
	sim_list = _get_sim_list()	
	i = random.randint(0, len(sim_list) - 1)

	return sim_list[i]


def _get_basic_properties(message_type):
	"""Returns AMPQ message properties."""
	return utils.create_ampq_message_properties(
		constants.PRODUCER_IGCM, 
		constants.APP_SMON, 
		message_type=message_type)


def _get_1000_content(
	activity, 
	compute_node,
	compute_node_login,
	compute_node_machine,
	execution_start_date,
	execution_state,
	experiment,
	model,
	name,
	space):

	return {
		'activity': activity,
		'compute_node': compute_node,
		'compute_node_login': compute_node_login,
		'compute_node_machine': compute_node_machine,
		'execution_start_date': execution_start_date,
		'execution_state': execution_state,
		'experiment': experiment,
		'model': model,
		'name': name,
		'space': space
	}


def _get_2000_content(name, state):
	"""Returns message content (type=2000)."""
	return {
		'event_type': 'state_change',
		'name': name,
		'state': state
	}


def get_message_1000(
	activity=_get_name(db.types.Activity), 
	compute_node=_get_name(db.types.ComputeNode),
	compute_node_login=_get_name(db.types.ComputeNodeLogin),
	compute_node_machine=_get_name(db.types.ComputeNodeMachine),
	execution_start_date= datetime.datetime.now(),
	execution_state=_get_name(db.types.SimulationState),
	experiment=_get_name(db.types.Experiment),
	model=_get_name(db.types.Model),
	name="test-" + str(uuid.uuid4())[:6],
	space=_get_name(db.types.ComputeNode)):
	"""Publishes a sim-mon message (type = 1000).

	"""
	props = _get_basic_properties(constants.TYPE_SMON_1000)

	content = _get_1000_content(
		activity, 
		compute_node, 
		compute_node_login, 
		compute_node_machine, 
		execution_start_date, 
		execution_state, 
		experiment, 
		model, 
		name, 
		space)

	return utils.MessageContextInfo(props, content)


def get_message_2000(
	name=_get_sim_name(), 
	state=_get_sim_state()):
	"""Publishes a sim-mon message (type = 2000).

	"""
	props = _get_basic_properties(constants.TYPE_SMON_2000)

	content = _get_2000_content(name, state)

	return utils.MessageContextInfo(props, content)


def get_message():
	yield get_message_1000()

	for i in range(3):
		yield get_message_1000()